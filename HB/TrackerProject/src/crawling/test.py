import time
import argparse

class test: 
    def __init__(self, **kwargs):
        index = kwargs.get('index',-1)
        print('Testing {}'.format(index))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--index', help='an integer to keep track of instances')
    args = parser.parse_args()
    a = test(index=args.index)
    