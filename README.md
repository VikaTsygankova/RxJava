# Задание 1 — мини-библиотека в духе RxJava (`pj-rx`)

## Архитектура

- **`Observer<T>`** — контракт приёма событий: `onNext` / `onError` / `onComplete`.
- **`Observable<T>`** — холодный поток: логика источника (`ObservableOnSubscribe`) выполняется **при каждой подписке** `subscribe`.
- **`Emitter<T>`** — интерфейс выдачи событий из `Observable.create`; **`SerialEmitter`** гарантирует один терминальный сигнал и перехват исключений из `onNext` наблюдателя.
- **`CancellableEmitter`** — обёртка для поддержки **`Disposable`** (отмена подписки, каскад через `attach` для внутренних подписок в `flatMap`).
- **Операторы** (`map`, `filter`, `flatMap`, `subscribeOn`, `observeOn`) реализованы как новые `Observable`, подписывающиеся на upstream.
- **`subscribeOn`** — запускает `subscribe` upstream-источника в потоке выбранного **`Scheduler`**.
- **`observeOn`** — переносит уведомления наблюдателю в поток планировщика через **очередь** (порядок событий для подписки сохраняется).
- **`Scheduler`** — `execute(Runnable)`; реализации: **`IOThreadScheduler`** (кэшируемый пул), **`SingleThreadScheduler`**, **`ComputationScheduler`** (фиксированный пул размером ≈ числу CPU). Фабрики: **`Schedulers.io()`**, **`single()`**, **`computation()`**.
- **`flatMap`** — merge внутренних потоков с **синхронизацией** выхода (`SynchronizedEmitter`) и учётом активных внутренних подписок (`wip`).

## Тестирование

Покрыты: базовый `create`, ошибки, `map`/`filter`, `subscribeOn`/`observeOn`, цепочка, `flatMap`, `Disposable` после завершения.

---

## Как проверить у себя

### 1. Требования

- **JDK 8+** (в проекте указана совместимость с Java 8).
- **Apache Maven 3.6+** в `PATH` (команда `mvn` в терминале).
- Убедитесь, что в системе есть **`javac`** (полный JDK), не только `java`:

  ```text
  javac -version
  ```

### 2. Сборка и тесты

В каталоге проекта `pj-rx` выполните:

```bash
mvn clean test
```

Ожидается: сборка без ошибок, все тесты **BUILD SUCCESS**.

Только компиляция без тестов:

```bash
mvn -q -DskipTests compile
```

### 3. Если `mvn` не найден

- Установите Maven и добавьте его `bin` в переменную окружения **PATH**, либо используйте встроенный Maven в **IntelliJ IDEA** / **Eclipse**: «Import as Maven project», затем запуск тестов через IDE.

### 4. Ошибка: «No compiler is provided… Perhaps you are running on a JRE rather than a JDK?»

Maven для `compile` вызывает **`javac`**, он есть только в **JDK**, не в отдельной **JRE**.

**Что сделать (Windows):**

1. Установите **JDK** (подойдёт 8, 11, 17 или 21), например [Eclipse Temurin](https://adoptium.net/) или Oracle JDK — главное, чтобы в каталоге установки была папка `bin` с файлами **`javac.exe`** и **`java.exe`**.

2. Задайте переменную окружения **`JAVA_HOME`** на корень JDK, например:
   - `C:\Program Files\Eclipse Adoptium\jdk-17.0.x.x-hotspot`
   - (не на `...\bin` и не на JRE внутри JRE-only установки).

3. В **`PATH`** путь к JDK должен идти **раньше** любых путей к старой JRE. Обычно добавляют:
   - `%JAVA_HOME%\bin`

4. Закройте и снова откройте терминал (или перезайдите в систему), затем проверьте:

   ```text
   where java
   where javac
   javac -version
   ```

   Команда `javac -version` должна печатать версию компилятора, а `where javac` — указывать на `...\jdk...\bin\javac.exe`, а не на отсутствие файла.

5. Снова в каталоге `pj-rx` выполните: `mvn clean test`.

**Без установки JDK из консоли:** в **IntelliJ IDEA** при открытии Maven-проекта укажите **Project SDK = JDK** (File → Project Structure → Project → SDK), затем запустите тесты через зелёные стрелки у классов `*Test` — IDE использует свой или выбранный JDK и обходит эту ошибку Maven вне IDE, если SDK настроен правильно.

### 5. Просмотр отчёта Surefire

После `mvn test` откройте:

```text
target/surefire-reports/
```

там — XML/HTML по каждому тест-классу.

---

## Формат сдачи (по условию курсовой)

- Исходный код библиотеки и тестов (этот проект).
- Отчёт: можно опираться на раздел «Архитектура» выше и дополнить сравнением с RxJava, описанием сценариев тестов и примерами цепочек операторов.
- Архив или **публичный** репозиторий со всеми файлами.
