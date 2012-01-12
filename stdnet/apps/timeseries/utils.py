from struct import pack, unpack

float_to_bin = lambda f : pack('>d', f)

bin_to_float = lambda f : unpack('>d', f)[0]