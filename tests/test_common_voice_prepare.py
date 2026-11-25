import shutil
import tempfile
import unittest
from pathlib import Path

from phraser.corpora.common_voice import prepare_common_voice_import as m

OLD_HEADER = ['client_id', 'path', 'sentence', 'up_votes', 'down_votes',
    'age', 'gender', 'accent']
NEW_HEADER = ['client_id', 'path', 'sentence_id', 'sentence',
    'sentence_domain', 'up_votes', 'down_votes', 'age', 'gender', 'accents',
    'variant', 'locale', 'segment']


def row(client_id, stem, up=2, down=0, age='', gender='', accent=''):
    return {'client_id': client_id, 'path': f'{stem}.mp3',
        'sentence': f'sentence {stem}', 'sentence_id': 'x',
        'sentence_domain': '', 'up_votes': str(up), 'down_votes': str(down),
        'age': age, 'gender': gender, 'accent': accent, 'accents': accent,
        'variant': '', 'locale': 'nl', 'segment': ''}


class PrepareTestCase(unittest.TestCase):
    def setUp(self):
        self.base = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.base, ignore_errors=True)

    def write(self, iso, header, rows, missing=(), no_duration=()):
        folder = self.base / iso
        folder.mkdir()
        lines = ['\t'.join(header)]
        for r in rows: lines.append('\t'.join(r[c] for c in header))
        (folder / 'validated.tsv').write_text('\n'.join(lines) + '\n')
        stems = [r['path'][:-4] for r in rows]
        present = [s for s in stems if s not in missing]
        (folder / 'clip_stems.txt').write_text('\n'.join(present) + '\n')
        durations = ['stem\tsample_rate\tn_channels\tn_samples\tduration_ms']
        for s in present:
            if s in no_duration: continue
            durations.append(f'{s}\t16000\t1\t16000\t1000')
        (folder / 'flac_durations.tsv').write_text('\n'.join(durations)
            + '\n')


class TestRead(PrepareTestCase):
    def test_both_header_shapes(self):
        for iso, header in (('eng', OLD_HEADER), ('nld', NEW_HEADER)):
            self.write(iso, header, [row('a', f'{iso}_1', accent='x')])
            rows = m.read_validated(iso, self.base)
            self.assertEqual(rows[0]['stem'], f'{iso}_1')
            self.assertEqual(rows[0]['accent'], 'x')

    def test_quotes_are_kept_literally(self):
        r = row('a', 'q1')
        r['sentence'] = '"Hallo", zei ze.'
        self.write('nld', NEW_HEADER, [r])
        rows = m.read_validated('nld', self.base)
        self.assertEqual(rows[0]['sentence'], '"Hallo", zei ze.')

    def test_unknown_language(self):
        with self.assertRaises(ValueError):
            m.language_dir('tur', self.base)


class TestSpeakers(unittest.TestCase):
    def test_most_common_non_empty(self):
        self.assertEqual(m.most_common_value(['', 'a', 'b', 'b', '']), 'b')
        self.assertEqual(m.most_common_value(['', '']), '')

    def test_speaker_info_and_conflicts(self):
        rows = [
            {'client_id': 's', 'gender': '', 'age': 'twenties',
                'accent': ''},
            {'client_id': 's', 'gender': 'male_masculine', 'age': 'twenties',
                'accent': 'x'},
            {'client_id': 's', 'gender': '', 'age': 'thirties',
                'accent': ''}]
        infos, conflicts = m.make_speaker_infos(rows, 'pol')
        self.assertEqual(infos['s'], {'name': 's', 'dataset': 'common_voice',
            'gender': 'male', 'age': 25, 'language': 'pol', 'region': 'x',
            'dialect': ''})
        self.assertEqual(conflicts, [{'client_id': 's', 'field': 'age',
            'values': ['thirties', 'twenties']}])


class TestSelect(unittest.TestCase):
    def setUp(self):
        self.speakers = {
            'big': {'gender': 'male', 'age': 25},
            'small': {'gender': 'unknown', 'age': 0}}
        self.clips = [{'client_id': 'big', 'stem': f'b{i}', 'up_votes': 2,
            'down_votes': 0} for i in range(5)]
        self.clips += [{'client_id': 'small', 'stem': f's{i}',
            'up_votes': 2 + i, 'down_votes': 0} for i in range(2)]

    def stems(self, **kwargs):
        selected = m.select_clips(self.clips, self.speakers, **kwargs)
        return [c['stem'] for c in selected]

    def test_keep_all(self):
        self.assertEqual(len(self.stems()), 7)

    def test_per_speaker_cap(self):
        self.assertEqual(self.stems(max_per_speaker=2),
            ['b0', 'b1', 's1', 's0'])

    def test_round_robin(self):
        '''Known gender/age ranks the big speaker first; votes order the
        small speaker's clips; each round takes one clip per speaker.'''
        self.assertEqual(self.stems(max_clips=5),
            ['b0', 's1', 'b1', 's0', 'b2'])

    def test_max_clips_above_total(self):
        self.assertEqual(len(self.stems(max_clips=100)), 7)


class TestPrepare(PrepareTestCase):
    def test_prepare(self):
        rows = [row('a', 'c1', gender='female', age='teens'),
            row('a', 'c2'), row('b', 'c3'), row('b', 'c4')]
        self.write('nld', NEW_HEADER, rows, missing=['c3'],
            no_duration=['c4'])
        result = m.prepare('nld', base=self.base)
        report = result['report']
        self.assertEqual(report['no_files'], ['c3'])
        self.assertEqual(report['no_duration'], ['c4'])
        self.assertEqual(report['n_selected'], 2)
        self.assertEqual([s['name'] for s in result['speakers']], ['a'])
        audio = result['audios'][0]
        self.assertEqual(audio['duration'], 1000)
        self.assertEqual(audio['language'], 'nld')
        self.assertTrue(audio['filename'].endswith('nld/clips/c1.flac'))
        clip = result['clips'][0]
        self.assertTrue(clip['textgrid_filename'].endswith(
            'nld/textgrids/c1.TextGrid'))

    def test_infos_build_models(self):
        from phraser import models
        self.write('eng', OLD_HEADER, [row('a', 'c1', gender='other',
            age='fourties', accent='England English')])
        result = m.prepare('eng', base=self.base)
        speaker = models.Speaker(**result['speakers'][0])
        audio = models.Audio(**result['audios'][0])
        self.assertEqual(speaker.gender(), 'other')
        self.assertEqual(speaker.age, 45)
        self.assertEqual(audio.duration, 1000)


if __name__ == '__main__':
    unittest.main()
