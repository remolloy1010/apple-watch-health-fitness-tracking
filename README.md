# Health and Fitness Metrics Tracking

## Steps to Reproduce

### Create Virtual Environment and Activate

1. Install virtualenv (if applicable)

```
python3.11 -m pip install virtualenv
```

2. Create virtual environment

```
python3.11 -m virtualenv venv
```

3. Activate virtual environment

```
source venv/bin/activate
```

# Notes / Reflection

- I needed to come up with a way to be flexible in how I transformed and stored my data, since there's too much data and it can change over time. Needed to pivot data

- Data cleaning: there were duplicates for some reason, which caused issues when pivoting - but also wanted to track and monitor that...To handle these, since it was only off by a tiny fraction (.00000001), I just took the last value for that row.

# Testing...
