## Запуск
```bash
# Компиляция
javac -d bin src/*.java

# Запуск (пример)
java -cp bin MapReduceMain input output 2 WordCountMapper WordCountReducer
```

## Структура
- `src/` - исходные коды
- `input/` - пример входных данных
- `output/` - результаты работы
