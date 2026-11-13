'''Small containment interpretation based on persisted sharing relationships.'''


def following_words(data):
    '''Index adjacent persisted Words within each phrase and speaker.'''
    groups = {}
    for word in data.words:
        group = (word.audio_id, word.speaker_id, word.parent_key)
        groups.setdefault(group, []).append(word)
    following = {}
    for words in groups.values():
        words.sort(key=lambda word: (word.start, word.end))
        for previous, current in zip(words, words[1:]):
            following[previous.key] = current
    return following


def explain_sharing(data, segment, parent, following):
    '''Explain only an owning Syllable's end crossing into a receiving Word.'''
    if segment.object_type != 'Syllable' or parent.object_type != 'Word':
        return False
    if segment.start < parent.start or not segment.shared_phone:
        return False
    if not parent.shared_phone or not _valid_child(segment, parent):
        return False
    phrase = data.parent_of(parent)
    if phrase is None or not _valid_child(parent, phrase): return False
    receiver = following.get(parent.key)
    if receiver is None or not receiver.missing_shared_phone: return False
    if not _valid_child(receiver, phrase): return False
    if receiver.start < parent.end or segment.end > receiver.end: return False
    phones = data.children.get(segment.identifier, [])
    if not phones or max(phone.end for phone in phones) != segment.end:
        return False
    crossing = [phone for phone in phones if phone.end > parent.end]
    if not crossing: return False
    for phone in phones:
        if phone.object_type != 'Phone' or not _valid_child(phone, segment):
            return False
        if phone.start < segment.start: return False
    for phone in crossing:
        if not phone.shared_phone or phone.start >= parent.end: return False
        if phone.end <= receiver.start: return False
    audio = data.audio_by_id.get(segment.audio_id)
    if audio is None or not audio.duration: return False
    return max(segment.end, receiver.end, phrase.end) <= audio.duration


def _valid_child(child, parent):
    if child.parent_key != parent.key or child.duration <= 0: return False
    if parent.duration <= 0 or child.audio_id != parent.audio_id: return False
    if child.speaker_id != parent.speaker_id: return False
    if parent.object_type == 'Phrase': return True
    return child.phrase_key == parent.phrase_key
