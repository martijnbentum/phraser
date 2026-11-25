'''Map Common Voice speaker metadata (gender, age) to phraser values.

Common Voice stores these per clip in validated.tsv; the values form a
closed set, so an unexpected value raises ValueError instead of being
stored as unknown. Choosing one value per speaker (contributors sometimes
change theirs) is up to the prepare step.
'''

# CV gender values; hun and pol releases use the male_masculine /
# female_feminine spelling, the others male / female / other.
CV_GENDER = {
    '': 'unknown',
    'male': 'male',
    'male_masculine': 'male',
    'female': 'female',
    'female_feminine': 'female',
    'other': 'other',
}

# CV age bands to their middle value in years. 'fourties' is CV's spelling;
# 'forties' is accepted as well.
CV_AGE = {
    '': 0,
    'teens': 15,
    'twenties': 25,
    'thirties': 35,
    'fourties': 45,
    'forties': 45,
    'fifties': 55,
    'sixties': 65,
    'seventies': 75,
    'eighties': 85,
    'nineties': 95,
}


def cv_gender(value):
    '''Return the phraser gender name (a utils.gender_dict key) for a CV
    gender value.'''
    return _lookup(CV_GENDER, value, 'gender')


def cv_age(value):
    '''Return the age in years for a CV age band, 0 when empty.

    NB: this is the middle of a ten-year band (teens -> 15), not the
    speaker's real age; Speaker.age holds it like an exact age.'''
    return _lookup(CV_AGE, value, 'age')


def _lookup(mapping, value, name):
    key = value.strip()
    if key in mapping: return mapping[key]
    raise ValueError(f'unknown Common Voice {name} value {value!r}')
