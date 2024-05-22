# Automatically generated, do not edit.

from handlers_base import MusicBrainzEventHandler


class ReleaseEventHandler(MusicBrainzEventHandler):

    @property
    def artwork_schema(self):
        return 'cover_art_archive'

    @property
    def artwork_table(self):
        return 'cover_art'

    @property
    def domain(self):
        return 'coverartarchive.org'

    @property
    def entity_schema(self):
        return 'musicbrainz'

    @property
    def entity_table(self):
        return 'release'

    @property
    def entity_type_kebab(self):
        return 'release'

    @property
    def entity_type_kebab(self):
        return 'release'

    @property
    def ia_collection(self):
        return 'coverartarchive'

    @property
    def perform_checks(self):
        return True

    @property
    def project_abbr(self):
        return 'caa'

    @property
    def ws_inc_params(self):
        return 'artists'


class EventEventHandler(MusicBrainzEventHandler):

    @property
    def artwork_schema(self):
        return 'event_art_archive'

    @property
    def artwork_table(self):
        return 'event_art'

    @property
    def domain(self):
        return 'eventartarchive.org'

    @property
    def entity_schema(self):
        return 'musicbrainz'

    @property
    def entity_table(self):
        return 'event'

    @property
    def entity_type_kebab(self):
        return 'event'

    @property
    def entity_type_kebab(self):
        return 'event'

    @property
    def ia_collection(self):
        return 'eventartarchive'

    @property
    def perform_checks(self):
        return False

    @property
    def project_abbr(self):
        return 'eaa'

    @property
    def ws_inc_params(self):
        return 'artist-rels+place-rels'


EVENT_HANDLER_CLASSES = {
    'release': ReleaseEventHandler,
    'event': EventEventHandler,
}
