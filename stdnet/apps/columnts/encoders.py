from struct import pack, unpack
from hashlib import sha1
 
from stdnet.utils import encoders, ispy3k

##########################################################
# flags
#
#    \x00    nan
#    \x01    int
#    \x02    float
#    \x03    small string (len <= 8)
#    \x04    big string (len > 8)
##########################################################

nil = b'\x00'*9
nil4 = b'\x00'*4
nan = float('nan')
float_to_bin = lambda f : pack('>d', f)
bin_to_float = lambda f : unpack('>d', f)[0]
int_to_bin = lambda f : pack('>i', f) + nil4
bin_to_int = lambda f : unpack('>i', f[:4])[0]


class DoubleEncoder(encoders.Default):
    
    def dumps(self, value):
        try:
            value = float(value)
            return b'\x02'+float_to_bin(value)
        except (TypeError,ValueError):
            return nil
    
    def loads(self, value): 
        if value[0] == 2:
            return bin_to_float(value[1:])
        else:
            return nan
        
            
class ValueEncoder(encoders.Default):
    value_length = 9
            
    def dumps(self, value):
        if value is None:
            return nil
        try:
            value = float(value)
            if value != value:
                return nil
            elif value == int(value):
                return b'\x01'+int_to_bin(int(value))
            else:
                return b'\x02'+float_to_bin(value)
        except ValueError:
            value = super(ValueEncoder,self).dumps(value)
            if len(value) <= 8:
                return b'\x03'+value
            else:
                val = b'\x04'+sha1(value).hexdigest(value)[:8].encode('utf-8')
                return val+value
    
    if ispy3k:
        
        def loads(self, value):
            flag = value[0]
            if flag == 0:
                return nan 
            elif flag == 1:
                return bin_to_int(value[1:])
            elif flag == 2:
                return bin_to_float(value[1:])
            else:
                return super(ValueEncoder,self).loads(value[1:])
    
    else:
        
        def loads(self, value):
            flag = ord(value[0])
            if flag == 0:
                return nan 
            elif flag == 1:
                return bin_to_int(value[1:])
            elif flag == 2:
                return bin_to_float(value[1:])
            else:
                return super(ValueEncoder,self).loads(value[1:])
            
