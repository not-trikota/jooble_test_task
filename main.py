import argparse
import os

from jooble_transformer.transformer import Transformer


parser = argparse.ArgumentParser(
    description='Transforms `train` and `test` tsvs.'
                ' See repo README for details on how to run from docker')
parser.add_argument('--data-path', default='/tmp/jooble_test_task_data')


if __name__ == '__main__':
    args = parser.parse_args()
    Transformer(data_path=args.data_path).transform()
