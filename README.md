# jooble_test_task

Transforms data according to
https://docs.google.com/document/d/1atst7jgkkoFr2Ub-l2oCNxpZdoszhNjBoLJUxOH_n1A/edit?usp=sharing

Outputs to the `data-path` folder (the one with `train.tsv` and `test.tsv`)

## Usage

#### Docker
```
docker build -t jooble_test_task .
docker run -v `pwd`/data:/tmp/jooble_test_task_data jooble_test_task
```

#### No Docker

```
# python3.7.7
pip install -r requirements.txt
python main.py --data-path data
```

## There is Spark approach to this task in `spark_approach.ipynb`
##### I have Spark running locally. Sorry no docker file for this one
