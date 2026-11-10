'''Tests for segment playback window and Segment.play wrappers.'''

import io
import types
import unittest
from contextlib import redirect_stdout
from unittest import mock

from phraser.audio import playback


def make_segment(start=1000, end=2000, duration=10000):
    audio = types.SimpleNamespace(filename='audio.wav', duration=duration)
    return types.SimpleNamespace(start=start, end=end, audio=audio,
        object_type='Word', label='w', children=[])


class TestPlayWindow(unittest.TestCase):
    def test_no_collar_converts_to_seconds(self):
        window = playback.play_window(make_segment())
        self.assertEqual(window, (1.0, 2.0))

    def test_collar_widens_window(self):
        segment = make_segment()
        window = playback.play_window(segment, collar=500)
        self.assertEqual(window, (0.5, 2.5))

    def test_collar_clamps_to_audio_bounds(self):
        segment = make_segment(start=200, end=9900)
        window = playback.play_window(segment, collar=500)
        self.assertEqual(window, (0.0, 10.0))

    def test_negative_collar_raises(self):
        segment = make_segment()
        with self.assertRaises(ValueError):
            playback.play_window(segment, collar=-1)

    def test_large_collar_warns(self):
        segment = make_segment(duration=3000)
        out = io.StringIO()
        with redirect_stdout(out):
            playback.play_window(segment, collar=2000)
        self.assertIn('Warning', out.getvalue())


class TestPlaySegment(unittest.TestCase):
    def test_forwards_window_to_play_audio(self):
        segment = make_segment()
        with mock.patch.object(playback.play, 'play_audio') as play_audio:
            with redirect_stdout(io.StringIO()):
                playback.play_segment(segment, collar=500, wait=True)
        play_audio.assert_called_once_with('audio.wav', start=0.5, end=2.5,
            wait=True)

    def test_play_children_plays_each_child_and_pauses(self):
        parent = make_segment()
        parent.children = [make_segment(0, 100), make_segment(100, 200)]
        with mock.patch.object(playback, 'play_segment') as play_segment, \
            mock.patch.object(playback.time, 'sleep') as sleep:
            playback.play_children(parent, collar=10, pause=0.1)
        self.assertEqual(play_segment.call_count, 2)
        play_segment.assert_called_with(parent.children[1], collar=10,
            wait=True)
        sleep.assert_called_with(0.1)


class TestSegmentWrappers(unittest.TestCase):
    def test_segment_play_forwards(self):
        from phraser import Word
        word = Word('w', 0, 10, audio_id=b'\x01' * 8,
            speaker_id=b'\x02' * 8)
        with mock.patch.object(playback, 'play_segment') as play_segment:
            word.play(collar=5, wait=True)
        play_segment.assert_called_once_with(word, collar=5, wait=True)

    def test_segment_play_children_forwards(self):
        from phraser import Word
        word = Word('w', 0, 10, audio_id=b'\x01' * 8,
            speaker_id=b'\x02' * 8)
        with mock.patch.object(playback, 'play_children') as play_children:
            word.play_children(collar=5, pause=0)
        play_children.assert_called_once_with(word, collar=5, pause=0)


if __name__ == '__main__':
    unittest.main()
