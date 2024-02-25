create virtual environment
```bash
python -m venv env
```

activate virtual environment
```bash
. ./env/bin/activate
```

install requirements
```bash
pip install -r requirements.txt
```

create ".env" file with db connection data
```
DB_HOST = "..."
DB_PORT = "..."
DB_NAME = "..."
DB_USER = "..."
DB_PASSWORD = "..."
```

run script
```bash
python main.py
```