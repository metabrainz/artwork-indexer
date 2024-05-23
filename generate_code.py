# artwork-indexer - update artwork index files at the Internet Archive
#
# Copyright (C) 2021  MetaBrainz Foundation
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

# flake8: noqa E501

import os
from textwrap import dedent
from projects import PROJECTS


curdir = os.path.dirname(__file__)

TG_OP_FULLNAMES = {
    'ins': 'INSERT',
    'upd': 'UPDATE',
    'del': 'DELETE',
}

indent_level = 1
def indent():
    global indent_level
    return ' ' * 4 * indent_level

handlers_source = dedent('''\
    # Automatically generated, do not edit.

    from handlers_base import MusicBrainzEventHandler


    ''')
handler_classes_source = ''
handler_classes_dict_source = 'EVENT_HANDLER_CLASSES = {\n'


for project in PROJECTS:
    abbr = project['abbr']
    art_schema = project['art_schema']
    art_table = project['art_table']
    entity_schema = project['entity_schema']
    entity_table = project['entity_table']
    entity_type = entity_table

    # q = qualified
    q_art_table = f'{art_schema}.{art_table}'
    q_entity_table = f'{entity_schema}.{entity_table}'
    # Note: cover_art_archive.image_type is also used by the
    # event_art_archive schema.
    q_image_type_table = 'cover_art_archive.image_type'

    extra_functions_source = ''
    extra_triggers_source = ''
    indent_level = 1

    for im in project['indexed_metadata']:
        im_schema = im['schema']
        im_table = im['table']
        q_im_table = f'{im_schema}.{im_table}'

        for tg_op in im['tg_ops']:
            extra_functions_source += f'\nCREATE OR REPLACE FUNCTION artwork_indexer.a_{tg_op}_{im_table}() RETURNS trigger AS $$\n'
            extra_functions_source += 'BEGIN\n'

            tg_rowvar = 'OLD' if tg_op == 'del' else 'NEW'

            col_comparisons = []
            if tg_op == 'upd':
                for col in im['indexed_columns']:
                    col_name = col['name']
                    if col['nullable']:
                        col_comparisons.append(f'OLD.{col_name} IS DISTINCT FROM NEW.{col_name}')
                    else:
                        col_comparisons.append(f'OLD.{col_name} != NEW.{col_name}')

            if col_comparisons:
                col_comparisons_source = ' OR '.join(col_comparisons)
                extra_functions_source += f'{indent()}IF ({col_comparisons_source}) THEN\n'
                indent_level += 1

            extra_functions_source += f'{indent()}INSERT INTO artwork_indexer.event_queue (entity_type, action, message)'

            indent_level += 1
            extra_functions_source += ' (\n'
            extra_functions_source += f"{indent()}SELECT '{entity_type}', 'index', jsonb_build_object('gid', {q_entity_table}.gid)\n"
            extra_functions_source += f'{indent()}FROM {q_entity_table}\n'

            for join in im.get('joins', ()):
                (lhs_schema, lhs_table, lhs_col) = join['lhs']
                (rhs_schema, rhs_table, rhs_col) = join['rhs']

                q_lhs_table = f'{lhs_schema}.{lhs_table}'
                q_rhs_table = f'{rhs_schema}.{rhs_table}'

                extra_functions_source += f'{indent()}JOIN {q_lhs_table} ON {q_lhs_table}.{lhs_col} = {q_rhs_table}.{rhs_col}\n'

            extra_functions_source += f'{indent()}WHERE EXISTS (\n'
            indent_level += 1
            extra_functions_source += f'{indent()}SELECT 1 FROM {q_art_table}\n'
            extra_functions_source += f'{indent()}WHERE {q_art_table}.{entity_type} = {q_entity_table}.id\n'
            indent_level -= 1
            extra_functions_source += f'{indent()})\n'

            if q_im_table == q_entity_table:
                extra_functions_source += f"{indent()}AND {q_entity_table}.gid = {tg_rowvar}.gid\n"

            im_condition = im.get('condition')
            if im_condition:
                extra_functions_source += f'{indent()}AND {im_condition.format(tg_rowvar=tg_rowvar)}\n'

            indent_level -= 1
            extra_functions_source += f'{indent()})\n'

            extra_functions_source += f'{indent()}ON CONFLICT DO NOTHING;\n'
            if col_comparisons:
                indent_level -= 1
                extra_functions_source += f'{indent()}END IF;\n'

            extra_functions_source += f'\n{indent()}RETURN {tg_rowvar};\n'
            extra_functions_source += 'END;\n'
            extra_functions_source += '$$ LANGUAGE plpgsql;\n'

            tg_fn_name = f'a_{tg_op}_{im_table}'
            tg_name = f'artwork_indexer_{tg_fn_name}'

            extra_triggers_source += dedent(f'''
                DROP TRIGGER IF EXISTS {tg_name} ON {q_im_table};

                CREATE TRIGGER {tg_name} AFTER {TG_OP_FULLNAMES[tg_op]}
                    ON {q_im_table} FOR EACH ROW
                    EXECUTE PROCEDURE artwork_indexer.{tg_fn_name}();
            ''')

    def index_artwork_stmt(gids, parent, starting_indent_level):
        global indent_level
        indent_level = starting_indent_level
        stmt = 'INSERT INTO artwork_indexer.event_queue ('
        stmt += 'entity_type, action, message'
        if parent:
            stmt += ', depends_on'
        stmt += ')\n'
        stmt += f'{indent()}VALUES '
        stmt += ', '.join([
            (
                f"('{entity_type}', 'index', jsonb_build_object('gid', {gid})" +
                (f", array[{parent}])" if parent else ')')
            ) for gid in gids
        ])
        stmt += '\n'
        stmt += f'{indent()}ON CONFLICT ';
        if parent:
            stmt += "(entity_type, action, message) WHERE state = 'queued'\n"
            stmt += f"{indent()}DO UPDATE SET depends_on = (coalesce(artwork_indexer.event_queue.depends_on, '{{}}') || {parent});"
        else:
            stmt += 'DO NOTHING;'
        return stmt

    def delete_artwork_stmt(artwork_id, gid, suffix, parent, return_var, starting_indent_level):
        global indent_level
        indent_level = starting_indent_level
        stmt = 'INSERT INTO artwork_indexer.event_queue ('
        stmt += 'entity_type, action, message'
        if parent:
            stmt += ', depends_on'
        stmt += ')\n'
        stmt += f"{indent()}VALUES ('{entity_type}', 'delete_image', "
        stmt += f"jsonb_build_object('artwork_id', {artwork_id}, 'gid', {gid}, 'suffix', {suffix})"
        if parent:
            stmt += f", array[{parent}]"
        stmt += ')\n'
        stmt += f'{indent()}RETURNING id INTO STRICT {return_var};'
        return stmt

    def deindex_artwork_stmt(gid, parent, starting_indent_level):
        global indent_level
        indent_level = starting_indent_level
        stmt = 'INSERT INTO artwork_indexer.event_queue (entity_type, action, message, depends_on)\n'
        stmt += f"{indent()}VALUES ('{entity_type}', 'deindex', jsonb_build_object('gid', {gid}), {parent})\n"
        stmt += f'{indent()}ON CONFLICT DO NOTHING;\n\n'
        # Delete any previous 'index' events that were queued; it's unlikely
        # these exist, but if they do we can avoid having them run and fail.
        stmt += f'{indent()}DELETE FROM artwork_indexer.event_queue\n'
        stmt += f"{indent()}WHERE state = 'queued'\n"
        stmt += f"{indent()}AND entity_type = '{entity_type}'\n"
        stmt += f"{indent()}AND action = 'index'\n"
        stmt += f"{indent()}AND message = jsonb_build_object('gid', {gid});"
        return stmt

    functions_source = dedent(f'''\
        -- Automatically generated, do not edit.

        CREATE OR REPLACE FUNCTION artwork_indexer.a_ins_{art_table}() RETURNS trigger AS $$
        DECLARE
            {entity_type}_gid UUID;
        BEGIN
            SELECT {q_entity_table}.gid
            INTO STRICT {entity_type}_gid
            FROM {q_entity_table}
            WHERE {q_entity_table}.id = NEW.{entity_type};

            {index_artwork_stmt((f'{entity_type}_gid',), None, 3)}

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION artwork_indexer.b_upd_{art_table}() RETURNS trigger AS $$
        DECLARE
            suffix TEXT;
            old_{entity_type}_gid UUID;
            new_{entity_type}_gid UUID;
            copy_event_id BIGINT;
            delete_event_id BIGINT;
        BEGIN
            SELECT {q_image_type_table}.suffix, old_{entity_type}.gid, new_{entity_type}.gid
            INTO STRICT suffix, old_{entity_type}_gid, new_{entity_type}_gid
            FROM {q_art_table}
            JOIN {q_image_type_table} USING (mime_type)
            JOIN {q_entity_table} old_{entity_type} ON old_{entity_type}.id = OLD.{entity_type}
            JOIN {q_entity_table} new_{entity_type} ON new_{entity_type}.id = NEW.{entity_type}
            WHERE {q_art_table}.id = OLD.id;

            IF OLD.{entity_type} != NEW.{entity_type} THEN
                -- The {entity_type} column changed, meaning two entities were merged.
                -- We'll copy the image to the new {entity_type} and delete it from
                -- the old one. The deletion event should have the copy event as its
                -- parent, so that it doesn't run until that completes.
                --
                -- We have no ON CONFLICT specifiers on the copy_image or delete_image,
                -- events, because they should *not* conflict with any existing event.

                INSERT INTO artwork_indexer.event_queue (entity_type, action, message)
                VALUES ('{entity_type}', 'copy_image', jsonb_build_object(
                    'artwork_id', OLD.id,
                    'old_gid', old_{entity_type}_gid,
                    'new_gid', new_{entity_type}_gid,
                    'suffix', suffix
                ))
                RETURNING id INTO STRICT copy_event_id;

                {delete_artwork_stmt('OLD.id', f'old_{entity_type}_gid', 'suffix', 'copy_event_id', 'delete_event_id', 4)}

                -- Check if any images remain for the old {entity_type}. If not, deindex it.
                PERFORM 1 FROM {q_art_table}
                WHERE {q_art_table}.{entity_type} = OLD.{entity_type}
                AND {q_art_table}.id != OLD.id
                LIMIT 1;

                IF FOUND THEN
                    -- If there's an existing, queued index event, reset its parent to our
                    -- deletion event (i.e. delay it until after the deletion executes).
                    {index_artwork_stmt((f'old_{entity_type}_gid', f'new_{entity_type}_gid'), 'delete_event_id', 5)}
                ELSE
                    {index_artwork_stmt((f'new_{entity_type}_gid',), 'delete_event_id', 5)}

                    {deindex_artwork_stmt(f'old_{entity_type}_gid', 'array[delete_event_id]', 5)}
                END IF;
            ELSE
                {index_artwork_stmt((f'old_{entity_type}_gid', f'new_{entity_type}_gid'), None, 4)}
            END IF;

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION artwork_indexer.b_del_{art_table}()
        RETURNS trigger AS $$
        DECLARE
            suffix TEXT;
            {entity_type}_gid UUID;
            delete_event_id BIGINT;
        BEGIN
            SELECT {q_image_type_table}.suffix, {q_entity_table}.gid
            INTO suffix, {entity_type}_gid
            FROM {q_entity_table}
            JOIN {q_image_type_table} ON {q_image_type_table}.mime_type = OLD.mime_type
            WHERE {q_entity_table}.id = OLD.{entity_type};

            -- If no row is found, it's likely because the entity itself has been
            -- deleted, which cascades to this table.
            IF FOUND THEN
                {delete_artwork_stmt('OLD.id', f'{entity_type}_gid', 'suffix', None, 'delete_event_id', 4)}

                {index_artwork_stmt((f'{entity_type}_gid',), 'delete_event_id', 4)}
            END IF;

            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION artwork_indexer.a_ins_{art_table}_type() RETURNS trigger AS $$
        DECLARE
            {entity_type}_gid UUID;
        BEGIN
            SELECT {q_entity_table}.gid
            INTO STRICT {entity_type}_gid
            FROM {q_entity_table}
            JOIN {q_art_table} ON {q_entity_table}.id = {q_art_table}.{entity_type}
            WHERE {q_art_table}.id = NEW.id;

            {index_artwork_stmt((f'{entity_type}_gid',), None, 3)}

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION artwork_indexer.b_del_{art_table}_type() RETURNS trigger AS $$
        DECLARE
            {entity_type}_gid UUID;
        BEGIN
            SELECT {q_entity_table}.gid
            INTO {entity_type}_gid
            FROM {q_entity_table}
            JOIN {q_art_table} ON {q_entity_table}.id = {q_art_table}.{entity_type}
            WHERE {q_art_table}.id = OLD.id;

            -- If no row is found, it's likely because the artwork itself has been
            -- deleted, which cascades to this table.
            IF FOUND THEN
                {index_artwork_stmt((f'{entity_type}_gid',), None, 4)}
            END IF;

            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION artwork_indexer.b_del_{entity_table}() RETURNS trigger AS $$
        BEGIN
            PERFORM 1 FROM {q_art_table}
            WHERE {q_art_table}.{entity_type} = OLD.id
            LIMIT 1;

            IF FOUND THEN
                INSERT INTO artwork_indexer.event_queue (entity_type, action, message) (
                    SELECT '{entity_type}', 'delete_image',
                        jsonb_build_object(
                            'artwork_id', {q_art_table}.id,
                            'gid', OLD.gid,
                            'suffix', {q_image_type_table}.suffix
                        )
                    FROM {q_art_table}
                    JOIN {q_image_type_table} USING (mime_type)
                    WHERE {q_art_table}.{entity_type} = OLD.id
                )
                ON CONFLICT DO NOTHING;

                {deindex_artwork_stmt('OLD.gid', 'NULL', 4)}
            END IF;

            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;
        ''')

    functions_source += extra_functions_source + '\n'

    functions_fpath = os.path.join(curdir, f'sql/{abbr}_functions.sql')
    with open(functions_fpath, 'w') as fp:
        fp.write(functions_source)

    triggers_source = dedent(f'''\
        -- Automatically generated, do not edit.

        SET LOCAL client_min_messages = warning;

        -- We drop the triggers first to simulate "CREATE OR REPLACE,"
        -- which isn't implemented for "CREATE TRIGGER."

        DROP TRIGGER IF EXISTS artwork_indexer_a_ins_{art_table} ON {art_schema}.{art_table};

        CREATE TRIGGER artwork_indexer_a_ins_{art_table} AFTER INSERT
            ON {art_schema}.{art_table} FOR EACH ROW
            EXECUTE PROCEDURE artwork_indexer.a_ins_{art_table}();

        DROP TRIGGER IF EXISTS artwork_indexer_b_upd_{art_table} ON {art_schema}.{art_table};

        CREATE TRIGGER artwork_indexer_b_upd_{art_table} BEFORE UPDATE
            ON {art_schema}.{art_table} FOR EACH ROW
            EXECUTE PROCEDURE artwork_indexer.b_upd_{art_table}();

        DROP TRIGGER IF EXISTS artwork_indexer_b_del_{art_table} ON {art_schema}.{art_table};

        CREATE TRIGGER artwork_indexer_b_del_{art_table} BEFORE DELETE
            ON {art_schema}.{art_table} FOR EACH ROW
            EXECUTE PROCEDURE artwork_indexer.b_del_{art_table}();

        DROP TRIGGER IF EXISTS artwork_indexer_a_ins_{art_table}_type ON {art_schema}.{art_table}_type;

        CREATE TRIGGER artwork_indexer_a_ins_{art_table}_type AFTER INSERT
            ON {art_schema}.{art_table}_type FOR EACH ROW
            EXECUTE PROCEDURE artwork_indexer.a_ins_{art_table}_type();

        DROP TRIGGER IF EXISTS artwork_indexer_b_del_{art_table}_type ON {art_schema}.{art_table}_type;

        CREATE TRIGGER artwork_indexer_b_del_{art_table}_type BEFORE DELETE
            ON {art_schema}.{art_table}_type FOR EACH ROW
            EXECUTE PROCEDURE artwork_indexer.b_del_{art_table}_type();

        DROP TRIGGER IF EXISTS artwork_indexer_b_del_{entity_table} ON {entity_schema}.{entity_table};

        CREATE TRIGGER artwork_indexer_b_del_{entity_table} BEFORE DELETE
            ON {entity_schema}.{entity_table} FOR EACH ROW
            EXECUTE PROCEDURE artwork_indexer.b_del_{entity_table}();
        ''')

    triggers_source += extra_triggers_source + '\n'

    triggers_fpath = os.path.join(curdir, f'sql/{abbr}_triggers.sql')
    with open(triggers_fpath, 'w') as fp:
        fp.write(triggers_source)

    handler_class_name = entity_type.title() + 'EventHandler'
    handler_classes_source += dedent(f'''\
        class {handler_class_name}(MusicBrainzEventHandler):

            @property
            def artwork_schema(self):
                return '{art_schema}'

            @property
            def domain(self):
                return '{project['domain']}'

            @property
            def entity_type(self):
                return '{entity_type}'

            @property
            def ia_collection(self):
                return '{project['ia_collection']}'

            @property
            def project_abbr(self):
                return '{project['abbr']}'

            @property
            def ws_inc_params(self):
                return '{project['ws_inc_params']}'


    ''')
    handler_classes_dict_source += \
        f"    '{entity_type}': {handler_class_name},\n"


handler_classes_dict_source += '}'
handlers_source += handler_classes_source
handlers_source += handler_classes_dict_source
handlers_source += '\n'

handlers_fpath = os.path.join(curdir, 'handlers.py')
with open(handlers_fpath, 'w') as fp:
    fp.write(handlers_source)
