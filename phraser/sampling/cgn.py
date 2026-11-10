'''Select CGN recordings using the comp-X/nl-or-vl directory layout.'''

from pathlib import Path

from . import _selection as selection


def _recording_metadata(filename):
    '''Validate the CGN component and variant in a recording path.'''
    path = Path(filename)
    component = path.parent.parent.name
    variant = path.parent.name
    if (not component.startswith('comp-') or len(component) != 6
        or component[-1] not in 'abcdefghijklmno'
        or variant not in ('nl', 'vl')):
        raise ValueError(f'unrecognized CGN recording path: {filename}')
    return component[-1], variant


def select_audios(audios=None, store=None, components=None, variant=None):
    '''Return CGN recordings matching component and variant restrictions.
    audios:      source recordings; None loads all audios from store
    store:       open Store, required when audios is None
    components:  included component letters, e.g. 'befg'; None allows all
    variant:     nl or vl; None allows both

    Validate every input path against comp-X/{nl,vl}/filename. Return all
    matches in key order; pass them to a sampling function or material copy.
    This uses paths rather than Audio.language, which is normally nld.
    '''
    if components is not None:
        components = set(components)
        if not components or not components <= set('abcdefghijklmno'):
            raise ValueError('components must contain CGN letters a through o')
    if variant is not None and variant not in ('nl', 'vl'):
        raise ValueError('variant must be nl, vl, or None')
    selected = []
    for audio in selection.get_audios(audios, store):
        component, recording_variant = _recording_metadata(audio.filename)
        if components is not None and component not in components: continue
        if variant is not None and recording_variant != variant: continue
        selected.append(audio)
    return sorted(selected, key=lambda audio: audio.key)
