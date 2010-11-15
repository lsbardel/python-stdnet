import sys

#make test TAGS="list"

if __name__ == '__main__':
    tags = sys.argv[1:]
    import stdnet
    stdnet.runtests(tags = tags)