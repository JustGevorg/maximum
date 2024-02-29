## Комментарий к доработке
В первоначальном варианте в where отсекал те communications, для которых у выбранных join-ом sessions 
не совпадает site_id с communication. 
В действительности, если верно понимаю, такое сценарий реален: с клиентом есть коммуникация, но сессия клиента была не
на том же сайте, что и коммуникация. 

Теперь для тех сессий, что выбираются для
каждой коммуникации вместо их данных в итоговой выборке могут быть NULL-ы при описанном выше сценарии.


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