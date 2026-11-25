import io
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from textgrid import IntervalTier, TextGrid

from phraser import Store
from phraser.corpora.common_voice import load_common_voice_to_db as m

# (start, end, ORT, KAN, MAS, MAU per phone) - MAUS SAMPA, pauses as CV has
NLD = [(0, .2, '', '', '<p:>', ['<p:>']),
    (.2, .5, 'dat', 'd A t', 'd A t', ['d', 'A', 't']),
    (.5, .7, '', '', '<p:>', ['<p:>'])]
ENG = [(0, .2, '', '', '<p:>', ['<p:>']),
    (.2, .5, 'her', 'h @`', 'h @`', ['h', '@`'])]


def write_textgrid(path, words):
    end = words[-1][1]
    tiers = {n: IntervalTier(n, 0, end) for n in
        ('ORT-MAU', 'KAN-MAU', 'KAS-MAU', 'MAU', 'MAS')}
    for start, stop, ort, kan, mas, phones in words:
        tiers['ORT-MAU'].add(start, stop, ort)
        tiers['KAN-MAU'].add(start, stop, kan)
        tiers['KAS-MAU'].add(start, stop, kan)
        tiers['MAS'].add(start, stop, mas)
        step = (stop - start) / len(phones)
        for i, phone in enumerate(phones):
            tiers['MAU'].add(round(start + i * step, 6),
                round(start + (i + 1) * step, 6), phone)
    tg = TextGrid(maxTime=end)
    for tier in tiers.values(): tg.append(tier)
    with open(path, 'w') as f: tg.write(f)


def prepared(folder, iso, stems_words, client_id='spk'):
    speakers = [{'name': client_id, 'dataset': 'common_voice',
        'gender': 'female', 'age': 25, 'language': iso, 'region': '',
        'dialect': ''}]
    audios, clips = [], []
    for stem, words in stems_words:
        tg = folder / f'{stem}.TextGrid'
        write_textgrid(tg, words)
        audio = str(folder / f'{stem}.flac')
        audios.append({'filename': audio, 'sample_rate': 16000,
            'n_channels': 1, 'duration': 700, 'dataset': 'common_voice',
            'language': iso, 'dialect': ''})
        clips.append({'stem': stem, 'client_id': client_id, 'sentence': '',
            'audio_filename': audio, 'textgrid_filename': str(tg)})
    return {'speakers': speakers, 'audios': audios, 'clips': clips}


class TestRelabel(unittest.TestCase):
    def setUp(self):
        self.folder = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.folder, ignore_errors=True)

    def load(self, words):
        path = self.folder / 'x.TextGrid'
        write_textgrid(path, words)
        tg = TextGrid.fromFile(str(path))
        return {t.name: [i.mark for i in t] for t in
            m.relabel_textgrid(tg, 'nld').tiers}

    def test_relabel(self):
        tiers = self.load(NLD)
        self.assertEqual(tiers['MAU'], ['(...)', 'd', 'ɑ', 't', '(...)'])
        self.assertEqual(tiers['MAS'], ['<p:>', 'd ɑ t', '<p:>'])
        self.assertEqual(tiers['KAN-MAU'], ['', 'd ɑ t', ''])
        self.assertEqual(tiers['ORT-MAU'], ['', 'dat', ''])

    def test_unknown_symbol_raises(self):
        with self.assertRaises(ValueError):
            self.load([(0, .3, 'x', 'Q9', 'Q9', ['Q9'])])


class TestLoad(unittest.TestCase):
    def setUp(self):
        self.folder = Path(tempfile.mkdtemp())
        with redirect_stdout(io.StringIO()):
            self.store = Store(path=self.folder / 'lmdb')

    def tearDown(self):
        with redirect_stdout(io.StringIO()):
            self.store.close()
        shutil.rmtree(self.folder, ignore_errors=True)

    def load(self, data, iso, chunk_size=1):
        with redirect_stdout(io.StringIO()):
            return m.load_prepared(data, iso, self.store,
                chunk_size=chunk_size)

    def phones(self):
        return sorted((p.label, p.position) for p in self.store.phones.all())

    def test_load_ipa_labels_and_positions(self):
        data = prepared(self.folder, 'nld', [('c1', NLD), ('c2', NLD)])
        report = self.load(data, 'nld')
        self.assertEqual((report['n_loaded'], report['errors']), (2, []))
        self.assertEqual(len(list(self.store.phrases.all())), 2)
        self.assertEqual(self.phones(), sorted(
            [('d', 'onset'), ('ɑ', 'nucleus'), ('t', 'coda')] * 2))
        audio = self.store.audios.get(filename=data['audios'][0]['filename'])
        speaker = self.store.speakers.get(name='spk', dataset='common_voice')
        self.assertEqual(audio.language, 'nld')
        self.assertEqual(sorted(a.filename for a in speaker.audios),
            sorted(a['filename'] for a in data['audios']))

    def test_language_table_used(self):
        '''ɚ is a vowel only in the English table.'''
        data = prepared(self.folder, 'eng', [('e1', ENG)])
        self.load(data, 'eng')
        self.assertEqual(self.phones(), [('h', 'onset'), ('ɚ', 'nucleus')])

    def test_rerun_skips(self):
        data = prepared(self.folder, 'nld', [('c1', NLD)])
        self.load(data, 'nld')
        report = self.load(data, 'nld')
        self.assertEqual((report['n_loaded'], report['n_skipped']), (0, 1))
        self.assertEqual(len(list(self.store.audios.all())), 1)
        self.assertEqual(len(list(self.store.phrases.all())), 1)

    def test_audio_without_phrase_is_reused(self):
        '''An audio saved before a crash, without its phrase, is reused.'''
        data = prepared(self.folder, 'nld', [('c1', NLD)])
        from phraser import models
        with redirect_stdout(io.StringIO()):
            self.store.save_many([models.Audio(**data['audios'][0],
                store=self.store)])
        report = self.load(data, 'nld')
        self.assertEqual(report['n_loaded'], 1)
        self.assertEqual(len(list(self.store.audios.all())), 1)
        self.assertEqual(len(list(self.store.phrases.all())), 1)

    def test_bad_clip_reported_and_not_saved(self):
        data = prepared(self.folder, 'nld', [('c1', NLD),
            ('bad', [(0, .3, 'x', 'Q9', 'Q9', ['Q9'])])])
        report = self.load(data, 'nld', chunk_size=10)
        self.assertEqual(report['n_loaded'], 1)
        self.assertEqual([e['stem'] for e in report['errors']], ['bad'])
        self.assertEqual(len(list(self.store.audios.all())), 1)


if __name__ == '__main__':
    unittest.main()
