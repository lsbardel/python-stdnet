from datetime import date, timedelta


def dategenerator(start, end, step = 1, desc = False):
    delta = timedelta(abs(step))
    end = max(start,end)
    if desc:
        dt = end
        while dt>=start:
            yield dt
            dt -= delta
    else:
        dt = start
        while dt<=end:
            yield dt
            dt += delta
            

def default_parse_interval(dt, delta = 0):
    if delta:
        return dt + timedelta(delta)
    else:
        return dt

