# Automatically generated, do not edit.

from handlers_base import MusicBrainzEventHandler


class ReleaseEventHandler(MusicBrainzEventHandler):

    @property
    def artwork_schema(self):
        return 'cover_art_archive'

    @property
    def domain(self):
        return 'coverartarchive.org'

    @property
    def entity_type(self):
        return 'release'

    @property
    def ia_collection(self):
        return 'coverartarchive'

    @property
    def ws_inc_params(self):
        return 'artists'


class EventEventHandler(MusicBrainzEventHandler):

    @property
    def artwork_schema(self):
        return 'event_art_archive'

    @property
    def domain(self):
        return 'eventartarchive.org'

    @property
    def entity_type(self):
        return 'event'

    @property
    def ia_collection(self):
        return 'eventartarchive'

    @property
    def ws_inc_params(self):
        return 'artist-rels+place-rels'


EVENT_HANDLER_CLASSES = {
    'release': ReleaseEventHandler,
    'event': EventEventHandler,
}
