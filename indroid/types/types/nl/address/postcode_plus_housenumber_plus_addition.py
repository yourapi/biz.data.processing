import postcode, postcode_plus_housenumber, housenumber_plus_addition
pattern = postcode.pattern[:-1] + housenumber_plus_addition.pattern

normalize = postcode_plus_housenumber.normalize