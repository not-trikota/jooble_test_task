import os
import pandas as pd

from .log import get_logger
logger = get_logger(__name__)


class Transformer:

    def __init__(self, data_path=None):
        self.data_path = data_path

    @property
    def output_path(self):
        return os.path.join(self.data_path, 'output.tsv')

    def transform(self):
        logger.info(f'Started transform. data_path={self.data_path}')

        features_metrics = self.get_features_metrics()
        logger.info(f'Calculated `features_metrics`'
                    f' = {{std,mean,count for all `feature_i_j`}}')
        logger.info(str(features_metrics['stds'])[:100])

        # its a bit ugly to transform and write in single function
        # yet it allows to process in chunks
        self.normalize_and_write(features_metrics)

    def get_features_metrics(self):
        # calculate mean,std,count for all features over train dataset
        train_path = os.path.join(self.data_path, 'train.tsv')

        features_metrics = {}
        def process_chunk(df):
            df = df[[c for c in df.columns
                    if c != 'id_job']].astype(float)
            self._update_features_metrics(df.count(), df.mean(), df.std(),
                                          features_metrics)
        self._process_features_file_in_chunks(train_path, process_chunk)

        return features_metrics

    def _process_features_file_in_chunks(self, path, process_chunk,
                                         chunk_size=100):
        # reads by chunk of lines into pd dataframe['id_job', 'feature_{i}_{j}']
        # calls process_chunk(chunk)
        ret = None
        with open(path) as f:
            next(f) # skip header
            parsed_lines = []
            for line in f:
                line_col_vals = line.strip().split('\t')
                parsed_line = {
                    'id_job': line_col_vals[0],
                }
                for col in line_col_vals[1:]:
                    col_id = col.split(',')[0]
                    parsed_line.update({
                        f'feature_{col_id}_{i}': i_v
                        for i, i_v in enumerate(col.split(',')[1:])
                    })
                parsed_lines.append(parsed_line)
                if len(parsed_lines) >= chunk_size:
                    ret = process_chunk(pd.DataFrame(parsed_lines))
                    parsed_lines = []
            if parsed_lines:
                ret = process_chunk(pd.DataFrame(parsed_lines))
        return ret

    def _update_features_metrics(self, counts, means, stds, features_metrics):
        # combine metrics of 2 groups in metrics for combined group
        if not features_metrics:
            features_metrics['counts'] = counts
            features_metrics['means'] = means
            features_metrics['stds'] = stds
            return
        old_counts = features_metrics['counts']
        old_means = features_metrics['means']
        old_stds = features_metrics['stds']
        
        features_metrics['counts'] = old_counts + counts
        features_metrics['means'] = (
            (old_means * old_counts + means * counts)
            / (old_counts + counts)
        )
        features_metrics['stds'] = (
            ((old_counts - 1) * old_stds ** 2
            + (counts - 1) * stds ** 2
            + old_counts * (old_means - features_metrics['means']) ** 2
            + counts * (means - features_metrics['means']) ** 2)
            / (old_counts + counts - 1)
        ) ** 0.5

    def _write_test_chunk(self, df):
        # write header if file is empty
        write_header = os.stat(self.output_path).st_size == 0

        df.to_csv(self.output_path, mode='a', header=write_header,
                  sep='\t', index=False)

    def process_test_chunk(self, df, features_metrics):
        df = df.astype(float)
        df.id_job = df.id_job.astype(int)
        
        feature_ids = set(col.split('_')[1] for col in df.columns
                        if col != 'id_job')
        for feature_id in feature_ids:
            feature_id_cols = [
                col for col in df.columns
                if col.startswith(f'feature_{feature_id}_')]
            feature_id_cols.sort(key=lambda id_col: int(id_col.split('_')[-1]))

            # get feature_{id}_stand array
            df_feature = df[feature_id_cols]
            for col in list(df.columns):
                if col == 'id_job':
                    continue
                df_feature[col] = (
                    (df_feature[col] - features_metrics['means'][col])
                    / features_metrics['stds'][col]
                )
            df[f'feature_{feature_id}_stand'] = (
                pd.Series(df_feature.values.tolist())
                  .apply(lambda l: ','.join(str(v) for v in l))
            )
            
            # get max index and abs_max_mean_diff
            df_feature = df[feature_id_cols]
            df[f'feature_{feature_id}_index'] = (
                df_feature.idxmax(axis=1)
                          .str.replace(f'feature_2_', '')
            )
            def get_max_feature_abs_mean_diff(row):
                idx_col = f'feature_{feature_id}_index'
                feature_col = f'''feature_{feature_id}_{row[idx_col]}'''
                return abs(row[feature_col] - features_metrics['means'][feature_col])
            df[f'max_feature_{feature_id}_abs_mean_diff'] = \
                df.apply(get_max_feature_abs_mean_diff, axis=1)
        write_cols = [c for c in df.columns
                      if not c.split('_')[-1].isnumeric()]
        self._write_test_chunk(df[write_cols])
        return df[write_cols]

    def normalize_and_write(self, features_metrics):
        # transform `test` dataset
        test_path = os.path.join(self.data_path, 'test.tsv')

        # create empty output file
        with open(self.output_path, 'w') as f:
            f.write('')

        # process test dataset
        self._process_features_file_in_chunks(
            test_path,
            lambda df: self.process_test_chunk(df, features_metrics),)

        return features_metrics
