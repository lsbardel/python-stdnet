import sys

if __name__ == '__main__':
    import stdnet
    tags = sys.argv[1:]
    stdnet.runbench(tags = tags)