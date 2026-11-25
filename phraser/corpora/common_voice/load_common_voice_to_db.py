'''Load a prepared Common Voice language into its own phraser store.

load_common_voice(iso) saves the speakers, then the clips in chunks: per
chunk the new Audio rows, their speaker-audio links and the segment trees
built from the TextGrids. A rerun skips clips whose audio already has a
phrase, so an interrupted load can be resumed; an audio saved without its
phrase (crash between the two writes) is reused.

TextGrid labels are MAUS SAMPA. relabel_textgrid maps the MAU, MAS and
KAN-MAU tiers to IPA with phone_mapper.maus before the segments are built,
because the builder assigns onset/nucleus/coda from the labels it reads.

estimate_store_size(iso) loads a small selection into a temporary store and
extrapolates bytes and time per clip to the whole language.
'''
import shutil
import tempfile
import time

from phone_mapper.maus import maus_sampa_to_ipa

from phraser import locations
from phraser import models
from phraser import Store
from phraser.storage import key_helper
from phraser.textgrids import textgrid_to_segments

from . import prepare_common_voice_import as prepare_module

DATASET = prepare_module.DATASET
DEFAULT_CHUNK_SIZE = 2000
# MAS pause label: the builder skips syllables with this mark, so it stays.
MAS_PAUSE = '<p:>'


def relabel_textgrid(tg, iso):
    '''Map the MAU, MAS and KAN-MAU labels of a loaded TextGrid from MAUS
    SAMPA to IPA in place. MAS and KAN-MAU hold space-separated symbols;
    empty labels and the MAS pause stay. An unmapped symbol raises
    ValueError.'''
    mapping = maus_sampa_to_ipa[iso]
    tiers = dict(zip(tg.getNames(), tg.tiers))
    for interval in tiers['MAU']:
        interval.mark = _map_symbol(mapping, interval.mark)
    for name in ('MAS', 'KAN-MAU'):
        for interval in tiers[name]:
            if interval.mark in ('', MAS_PAUSE): continue
            symbols = interval.mark.split()
            interval.mark = ' '.join(_map_symbol(mapping, s) for s in symbols)
    return tg


def _map_symbol(mapping, symbol):
    if symbol in mapping: return mapping[symbol]
    raise ValueError(f'no IPA for MAUS symbol {symbol!r}')


def open_store(iso, path=None):
    '''Open (or create) the store for a language.'''
    if path is None: path = locations.common_voice_lmdb(iso)
    return Store(path=path)


def save_speakers(speaker_infos, store):
    '''Save speakers not yet in the store; return {name: Speaker}.'''
    store.refresh_query_roots()
    existing = {s.name: s for s in store.speakers.filter(dataset=DATASET)}
    new = []
    for info in speaker_infos:
        if info['name'] in existing: continue
        speaker = models.Speaker(**info, save=False, store=store)
        existing[speaker.name] = speaker
        new.append(speaker)
    store.save_many(new)
    print(f'speakers: {len(new)} added, {len(speaker_infos) - len(new)} '
        'already stored')
    return existing


def existing_audios(store):
    '''Return {filename: Audio} for the Common Voice audios in the store.'''
    store.refresh_query_roots()
    return {a.filename: a for a in store.audios.filter(dataset=DATASET)}


def has_phrase(audio, store):
    return next(store.DB.audio_id_to_child_keys(audio.identifier),
        None) is not None


def build_clip(clip, audio, speaker, iso, store):
    '''Return the staged segment items for one clip.'''
    filename = clip['textgrid_filename']
    tg = relabel_textgrid(textgrid_to_segments.load_textgrid(filename), iso)
    return textgrid_to_segments.textgrid_filename_to_database_objects(
        filename, audio=audio, speaker=speaker, multiple_speakers=False,
        store=store, textgrid=tg)


def load_chunk(clips, audio_infos, speakers, audios, iso, store):
    '''Build and save one chunk; return (n_loaded, n_skipped, errors).
    audios ({filename: Audio}) is updated with the new audios.'''
    new_audios, links, items, errors, n_skipped = [], [], [], [], 0
    for clip, info in zip(clips, audio_infos):
        audio = audios.get(info['filename'])
        if audio is not None and has_phrase(audio, store):
            n_skipped += 1
            continue
        is_new = audio is None
        if is_new: audio = models.Audio(**info, save=False, store=store)
        speaker = speakers[clip['client_id']]
        try: clip_items = build_clip(clip, audio, speaker, iso, store)
        except (ValueError, KeyError, OSError) as e:
            errors.append({'stem': clip['stem'], 'error': repr(e)})
            continue
        if is_new:
            new_audios.append(audio)
            audios[audio.filename] = audio
        links.append(key_helper.speaker_audio_link(speaker, audio))
        items.extend(clip_items)
    store.save_many(new_audios)
    if links: store.DB.write_many_speaker_audio_links(links)
    store.save_many(items)
    n_loaded = len(clips) - n_skipped - len(errors)
    return n_loaded, n_skipped, errors


def load_prepared(prepared, iso, store, chunk_size=DEFAULT_CHUNK_SIZE):
    '''Load prepare() output into store; return a load report.'''
    start = time.time()
    speakers = save_speakers(prepared['speakers'], store)
    audios = existing_audios(store)
    clips, audio_infos = prepared['clips'], prepared['audios']
    n_loaded, n_skipped, errors = 0, 0, []
    for i in range(0, len(clips), chunk_size):
        loaded, skipped, chunk_errors = load_chunk(clips[i:i + chunk_size],
            audio_infos[i:i + chunk_size], speakers, audios, iso, store)
        n_loaded += loaded
        n_skipped += skipped
        errors.extend(chunk_errors)
        done = min(i + chunk_size, len(clips))
        minutes = (time.time() - start) / 60
        print(f'{iso}: {done}/{len(clips)} clips, {n_loaded} loaded, '
            f'{n_skipped} skipped, {len(errors)} errors, {minutes:.1f} min',
            flush=True)
    store.refresh_query_roots()
    return {'iso': iso, 'n_loaded': n_loaded, 'n_skipped': n_skipped,
        'errors': errors, 'seconds': time.time() - start}


def load_common_voice(iso, store=None, max_clips=None, max_per_speaker=None,
    chunk_size=DEFAULT_CHUNK_SIZE, base=None):
    '''Prepare and load one language; return (prepare report, load report).
    store defaults to locations.common_voice_lmdb(iso).'''
    prepared = prepare_module.prepare(iso, max_clips=max_clips,
        max_per_speaker=max_per_speaker, base=base)
    prepare_module.print_report(prepared['report'])
    if store is None: store = open_store(iso)
    report = load_prepared(prepared, iso, store, chunk_size=chunk_size)
    return prepared['report'], report


def store_bytes(store):
    '''Bytes in use by the store's LMDB environment.'''
    info, stat = store.DB.env.info(), store.DB.env.stat()
    return (info['last_pgno'] + 1) * stat['psize']


def estimate_store_size(iso, n_clips=500, max_per_speaker=None, base=None):
    '''Load n_clips (round-robin over speakers) into a temporary store and
    extrapolate size and load time to the full selection.'''
    full = prepare_module.prepare(iso, max_per_speaker=max_per_speaker,
        base=base)
    n_total = len(full['clips'])
    sample = prepare_module.prepare(iso, max_clips=n_clips,
        max_per_speaker=max_per_speaker, base=base)
    tmp = tempfile.mkdtemp(prefix=f'cv_{iso}_estimate_')
    store = None
    try:
        store = Store(path=tmp)
        empty = store_bytes(store)
        report = load_prepared(sample, iso, store)
        n = report['n_loaded']
        if n == 0: raise ValueError('no clips loaded in the sample')
        bytes_per_clip = (store_bytes(store) - empty) / n
        seconds_per_clip = report['seconds'] / n
    finally:
        if store is not None: store.close()
        shutil.rmtree(tmp, ignore_errors=True)
    estimate = {'iso': iso, 'n_sample': n, 'n_total': n_total,
        'bytes_per_clip': bytes_per_clip,
        'estimated_gb': bytes_per_clip * n_total / 1e9,
        'seconds_per_clip': seconds_per_clip,
        'estimated_hours': seconds_per_clip * n_total / 3600}
    print(f"{iso}: {bytes_per_clip / 1e3:.1f} kB/clip -> "
        f"{estimate['estimated_gb']:.1f} GB for {n_total} clips; "
        f"{seconds_per_clip * 1e3:.0f} ms/clip -> "
        f"{estimate['estimated_hours']:.1f} h")
    return estimate
