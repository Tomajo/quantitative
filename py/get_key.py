import sys
def get_key():
    try:
        f = open("resources/key.txt", "r")
    except OSError:
        print ("Could not open/read file:", fname)
        sys.exit()

    with f:
        return f.read()

