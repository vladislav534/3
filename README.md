# UniSchedule — Университетское расписание

Кроссплатформенное приложение (iOS + Android) для студентов.

## Быстрый старт (macOS)

### Требования
- **macOS** (для сборки iOS)
- **Xcode** (App Store, бесплатно)
- **Flutter SDK** (`brew install flutter`)
- **CocoaPods** (`brew install cocoapods`)

### Установка

```bash
# 1. Клонируй репозиторий
git clone <repo-url>
cd uni_schedule

# 2. Запусти скрипт настройки
chmod +x setup.sh
./setup.sh

# 3. Запусти на симуляторе
flutter run
```

### Или вручную

```bash
# Генерация платформенных файлов
flutter create --project-name uni_schedule --org com.unischedule .

# Установка зависимостей
flutter pub get

# iOS зависимости
cd ios && pod install && cd ..

# Запуск
flutter run
```

## Запуск на реальном iPhone

1. Подключи iPhone к Mac кабелем
2. Открой `ios/Runner.xcworkspace` в Xcode
3. Выбери свой Apple ID в **Signing & Capabilities** (бесплатный аккаунт подойдёт для тестирования)
4. Выбери своё устройство и нажми Run

Или через терминал:
```bash
flutter devices          # посмотреть подключённые устройства
flutter run -d <id>      # запустить на устройстве
```

## Публикация в TestFlight

```bash
flutter build ipa
```
Затем загрузи `.ipa` через **Transporter** (бесплатное приложение из App Store) в App Store Connect.

## Структура проекта

```
lib/
├── main.dart                  # Точка входа
├── app.dart                   # Роутинг
├── models/                    # Модели данных
├── services/                  # Бизнес-логика
├── providers/                 # State management
├── screens/                   # Экраны
├── widgets/                   # Переиспользуемые виджеты
├── theme/                     # Тема приложения
└── utils/                     # Утилиты
```

## Админ-доступ (для тестирования)
- Email: `admin@unischedule.ru`
- Пароль: `admin123`
