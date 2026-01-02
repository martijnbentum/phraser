import models
import random
from progressbar import progressbar



def split_syllables(ipa): 
    return [s.strip() for s in ipa.split(".")]

def split_phones(syl):
    return [p for p in syl.split() if p]


# -------------------------- GENERATOR -------------------------

def generate_objects():
    """
    Returns:
        list of Phrase objects (each with embedded words/syllables/phones)
    """
    phrase_objects = []
    time_cursor = 0.0
    all_objects = []
    models.cache.turn_off_db_saving()
    print(f'cache saving is {models.cache.is_db_saving_allowed()}')

    for speaker_name in progressbar(SPEAKER_NAMES):
        speaker = models.Speaker(speaker_name)
        all_objects.append(speaker)
    
        for i, text in enumerate(PHRASES):
            # Audio + Speaker
            filename = f"utt_{i:04d}.wav"
            audio = models.Audio(filename, duration=8.0)
            all_objects.append(audio)
            audio.add_speaker(speaker)

            words = text.split()
            ph_start = time_cursor
            ph_end   = ph_start + len(words) * 0.6 + 1.0

            # ------------------- Create Phrase -------------------
            phrase = models.Phrase(text, start=ph_start, end=ph_end)
            all_objects.append(phrase)
            audio.add_phrase(phrase)
            phrase.add_speaker(speaker)

            # -------------------- Add WORDS -----------------------
            w_cursor = ph_start

            for word in words:
                ipa = IPA[word.lower()]
                sylls = split_syllables(ipa)

                w_start = w_cursor
                w_end   = w_start + 0.5 + 0.04 * len(sylls)
                w_cursor = w_end + 0.05

                word_obj = models.Word(word, start=w_start, end=w_end)
                all_objects.append(word_obj)
                phrase.add_child(word_obj)

                # ---------------- SYLLABLES ---------------------
                syl_dur = (w_end - w_start) / len(sylls)
                s_cursor = w_start

                for syl in sylls:
                    s_start = s_cursor
                    s_end   = s_start + syl_dur
                    s_cursor += syl_dur

                    syl_obj = models.Syllable(syl, start=s_start, end=s_end)
                    word_obj.add_child(syl_obj)
                    all_objects.append(syl_obj)

                    # ---------------- PHONES --------------------
                    phones = split_phones(syl)
                    ph_dur = (s_end - s_start) / len(phones)
                    ph_cursor = s_start

                    for ph in phones:
                        p_start = ph_cursor
                        p_end   = p_start + ph_dur
                        ph_cursor += ph_dur

                        phone_obj = models.Phone(ph, start=p_start, end=p_end)
                        syl_obj.add_child(phone_obj)
                        all_objects.append(phone_obj)

            phrase_objects.append(phrase)
            time_cursor = ph_end + 1.0

    
    print(f'generated total objects:', len(all_objects))
    models.cache.turn_on_db_saving()
    print(f'cache saving is {models.cache.is_db_saving_allowed()}')
    models.cache.save_many(all_objects)

    return phrase_objects

PHRASES = [
    "i like green apples",
    "people often forget their keys",
    "the cat sat on the mat",
    "hello world",
    "a quick brown fox jumps over the lazy dog",
    "the weather was surprisingly warm",
    "yesterday the weather was warm",
    "i like the green dog",
    "many researchers investigate complex linguistic structures",
    "people forget keys in the morning",
    "the dog sat on the green mat",
    "the fox jumps over the dog",
    "a quick cat jumps over the mat",
    "green apples taste warm",
    "people like warm apples",
    "i often forget my keys",
    "the cat likes apples",
    "researchers investigate linguistic patterns",
    "complex structures often surprise researchers",
    "linguistic structures are complex",
    "people investigate complex ideas",
    "the morning was surprisingly warm",
    "the green fox sat on the dog",
    "the brown dog forgets the cat",
    "i like warm weather",
    "many people forget their apples",
    "researchers analyze linguistic structure",
    "the quick fox surprises the cat",
    "green apples fall in the morning",
    "warm weather surprises people",
    "i like brown dogs",
    "the lazy dog sat on the warm mat",
    "linguistic researchers like complex ideas",
    "the weather was warm yesterday",
    "people analyze warm apples",
    "the fox was surprisingly quick",
    "i forget the green keys",
    "the cat was lazy yesterday",
    "researchers often analyze structures",
    "complex linguistic ideas surprise people",
    "green apples surprise the cat",
    "the brown fox likes warm apples",
    "many people analyze complex data",
    "the weather often surprises researchers",
    "linguistic structures can be complex",
    "the quick dog jumps on the mat",
    "i analyze linguistic patterns",
    "people like the warm morning",
    "many researchers like warm weather",
    "the fox forgets the green keys",
    "the dog likes complex structures",
    "warm apples taste surprisingly good",
    "linguistic patterns are often complex",
    "i like quick dogs",
    "the lazy cat sat in the morning",
    "researchers forget their warm apples",
    "people investigate warm weather",
    "the green cat jumps quickly",
    "many people like green weather",
    "the dog was quick yesterday",
    "warm morning weather surprises people",
    "the brown dog sat in the warm morning",
    "i like linguistic research",
    "the fox likes warm weather",
    "people analyze complex linguistic structures",
    "researchers like quick linguistic analysis",
    "the cat jumps over complex structures",
    "many quick dogs jump over cats",
    "i like brown apples",
    "warm apples taste good",
    "i like warm apples",
    "warm weather surprises people",
    "many people like warm weather",
    "the cat sat in the morning",
    "the dog was quick",
    "quick dogs jump over the cat",
    "the green cat jumps quickly",
    "people investigate complex patterns",
    "linguistic patterns are complex",
    "researchers analyze linguistic patterns",
    "i like linguistic patterns",
    "the brown dog sat in the morning",
    "warm morning surprises people",
    "many people investigate weather",
    "the fox likes warm weather",
    "researchers forget their apples",
    "warm apples surprise people",
    "quick dogs like warm weather",
    "the lazy cat sat",
    "people analyze complex structures",
    "linguistic structures are complex",
    "researchers like linguistic analysis",
    "i like quick dogs",
    "many dogs jump over the cat",
    "the dog sat in the morning",
    "warm weather was good",
    "apples taste surprisingly good",
    "people like green weather",
    "the cat jumps over the dog",
    "researchers investigate linguistic structures",
    "quick dogs were not yesterday",
    "the brown dog was quick",
    "i like brown apples",
    "people forget their warm apples",
    "the green cat sat in the morning",
    "many people like apples",
    "researchers analyze complex analysis",
    "warm morning weather surprises people",
    "the fox jumps quickly",
    "linguistic analysis is complex",
    "quick dogs jump over complex structures",
    "people investigate linguistic patterns",
    "the dog jumps quickly",
    "researchers like quick analysis",
    "many people analyze patterns",
    "warm apples were surprisingly good",
    "the lazy dog sat in the morning",
    "green weather surprises people",
    "i like warm weather",
    "the cat likes warm apples",
    "people analyze linguistic analysis",
    "complex patterns surprise people",
    "researchers investigate warm weather",
    "the brown dog jumps over the cat",
    "quick dogs like apples",
    "warm weather surprises researchers",
    "the fox likes apples",
    "many researchers analyze structures",
    "i like complex linguistic patterns"
]


IPA = {
    "hello":         "h ə . l oʊ",
    "world":         "w ɜː r l d",

    "the":           "ð ə",
    "cat":           "k æ t",
    "sat":           "s æ t",
    "on":            "ɒ n",
    "mat":           "m æ t",

    "a":             "ə",
    "quick":         "k w ɪ k",
    "brown":         "b r aʊ n",
    "fox":           "f ɒ k s",
    "jumps":         "dʒ ʌ m p s",
    "over":          "oʊ . v ər",
    "lazy":          "l eɪ . z i",
    "dog":           "d ɒ ɡ",

    "i":             "aɪ",
    "like":          "l aɪ k",
    "green":         "ɡ r iː n",
    "apples":        "æ . p ə l z",

    "people":        "p iː . p əl",
    "often":         "ɒ f . t ən",
    "forget":        "f ər . ɡ ɛ t",
    "their":         "ð ɛə r",
    "keys":          "k iː z",
    "in":            "ɪ n",
    "morning":       "m ɔː r . n ɪ ŋ",

    "yesterday":     "j ɛ s . t ə . d eɪ",
    "weather":       "w ɛ ð . ər",
    "was":           "w ɒ z",
    "surprisingly":  "s ə . p r aɪ z . ɪ ŋ . l i",
    "warm":          "w ɔː r m",

    "many":          "m ɛ . n i",
    "researchers":   "r iː . s ɜː r . tʃ ər z",
    "investigate":   "ɪ n . v ɛ s . t ɪ . ɡ eɪ t",
    "complex":       "k ɒ m . p l ɛ k s",
    "linguistic":    "l ɪ ŋ . ɡ w ɪ s . t ɪ k",
    "structures":    "s t r ʌ k . tʃ ər z",
    "i":              "aɪ",
    "like":           "l aɪ k",
    "green":          "ɡ r iː n",
    "apples":         "æ . p ə l z",

    "people":         "p iː . p əl",
    "often":          "ɒ f . t ən",
    "forget":         "f ər . ɡ ɛ t",
    "their":          "ð ɛə r",
    "keys":           "k iː z",

    "the":            "ð ə",
    "cat":            "k æ t",
    "sat":            "s æ t",
    "on":             "ɒ n",
    "mat":            "m æ t",

    "hello":          "h ə . l oʊ",
    "world":          "w ɜː r l d",

    "a":              "ə",
    "quick":          "k w ɪ k",
    "brown":          "b r aʊ n",
    "fox":            "f ɒ k s",
    "jumps":          "dʒ ʌ m p s",
    "over":           "oʊ . v ər",
    "lazy":           "l eɪ . z i",
    "dog":            "d ɒ ɡ",

    "weather":        "w ɛ ð . ər",
    "was":            "w ɒ z",
    "surprisingly":   "s ə . p r aɪ z . ɪ ŋ . l i",
    "warm":           "w ɔː r m",

    "yesterday":      "j ɛ s . t ə . d eɪ",
    "morning":        "m ɔː r . n ɪ ŋ",

    "many":           "m ɛ . n i",
    "researchers":    "r iː . s ɜː r . tʃ ər z",
    "investigate":    "ɪ n . v ɛ s . t ɪ . ɡ eɪ t",
    "complex":        "k ɒ m . p l ɛ k s",
    "linguistic":     "l ɪ ŋ . ɡ w ɪ s . t ɪ k",
    "structures":     "s t r ʌ k . tʃ ər z",

    "patterns":       "p æ . t ə n z",
    "ideas":          "aɪ . d iː . ə z",
    "surprise":       "s ər . p r aɪ z",
    "surprises":      "s ər . p r aɪ . z ɪ z",
    "taste":          "t eɪ s t",
    "good":           "ɡ ʊ d",
    "can":            "k æ n",
    "be":             "b iː",

    "words":          "w ɜː r d z",
    "fall":           "f ɔː l",
    "are":            "ɑː r",
    "quickly":        "k w ɪ k . l i",
    "my":             "m aɪ",
    "dogs":           "d ɒ ɡ z",
    "cats":           "k æ t s",
    "in":             "ɪ n",

    "analysis":       "ə . n æ . l ə . s ɪ s",
    "analyze":        "æ . n ə . l aɪ z",
    "data":           "d eɪ . t ə",
    "research":       "r iː . s ɜː r tʃ",
    "investigates":   "ɪ n . v ɛ s . t ɪ . ɡ eɪ t s",
    "likes":       "l aɪ k s",
    "forgets":     "f ə r . ɡ ɛ t s",
    "structure":   "s t r ʌ k . tʃ ər",
    "jump":        "dʒ ʌ m p",
    "is":          "ɪ z",
    "surprising":  "s ər . p r aɪ . z ɪ ŋ",
    "analyzes":    "æ . n ə . l aɪ . z ɪ z",
    "were":        "w ɜː ",
    "not":         "n ɒ t",
}


SPEAKER_NAMES = 'Alice Emma Olivia Ava Sophia Isabella Mia Charlotte Amelia Harper Evelyn Abigail Emily Ella Grace Chloe Madison Aria Scarlett Lily Hannah Zoe Nora Stella Aubrey Natalie Zoey Leah Hazel Victoria Riley Savannah Brooklyn Claire Audrey Anna Lucy Samantha Maya Caroline Sarah Kennedy Allison Skylar Gabriella Violet Eleanor Penelope Paisley Liam Noah William Terry Doda Neo Scarlett Tiff Duffy AMy Damm Dodo Nonu Zazu Oliver James Benjamin Lucas Henry Alexander Elijah Daniel Matthew Samuel Jack Sebastian Theodore Owen Julian Levi Isaac Caleb Ezra Nathan Aaron Joseph David Christopher Jonathan Anthony Nicholas Andrew Thomas Michael'.split(' ')
