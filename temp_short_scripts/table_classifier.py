import pandas as pd
import statsmodels.api as sm
import numpy as np
from dataset_hardcodes import build_training_data
from scrapes import extractions


def ml():
    trainset = build_training_data()
    print(trainset.__len__())

    labels = ['is_horizontal', 'index','is_table','is_aside','has_right_property','depth_table','tf_idf','no_rows',
              'max_no_col','texttag_ratio','has_th','has_img','has_infobox']
    df = pd.DataFrame.from_records(trainset, columns=labels)
    print(df.head())
    cols_to_keep = ['is_horizontal', 'index', 'has_right_property','depth_table','tf_idf','no_rows',
              'max_no_col','texttag_ratio','has_th','has_img','has_infobox']
    # Index([gre, gpa, prestige_2, prestige_3, prestige_4], dtype=object)
    data = df[cols_to_keep]
    train_cols = data.columns[1:]
    print(data.head())
    logit = sm.Logit(data['is_horizontal'], data[train_cols])

    # fit the model
    result = logit.fit(method='bfgs')
    print(result.summary())
    print(result.conf_int())
    print(np.exp(result.params))


if __name__ == "__main__":
    extractions()
    ml()
    #classify()
