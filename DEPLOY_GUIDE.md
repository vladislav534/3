# Пошаговая инструкция: от кода до iPhone

## Часть 1: Установка (один раз, ~30 минут)

### 1.1 Установи Xcode
- Открой **App Store** на Mac
- Найди **Xcode**, нажми **Установить** (бесплатно, ~12 ГБ)
- После установки открой Xcode один раз, прими лицензию
- Открой **Терминал** (Cmd+Space → "Terminal") и выполни:
```bash
sudo xcode-select --install
sudo xcodebuild -license accept
```

### 1.2 Установи Homebrew (если нет)
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### 1.3 Установи Flutter и CocoaPods
```bash
brew install flutter cocoapods
```

### 1.4 Проверь что всё работает
```bash
flutter doctor
```
Должно показать зелёные галочки напротив Flutter и Xcode.
Если есть ошибки — flutter doctor подскажет что исправить.

---

## Часть 2: Сборка проекта (~10 минут)

### 2.1 Склонируй репозиторий
```bash
cd ~/Desktop
git clone https://github.com/vladislav534/3.git UniSchedule
cd UniSchedule
```

### 2.2 Создай платформенные файлы
```bash
flutter create --project-name uni_schedule --org com.unischedule .
```

### 2.3 Установи зависимости
```bash
flutter pub get
cd ios && pod install && cd ..
```

### 2.4 Настрой iOS-разрешения для уведомлений
Открой файл `ios/Runner/Info.plist` в текстовом редакторе и
ПЕРЕД последним `</dict>` добавь:

```xml
<key>UIBackgroundModes</key>
<array>
    <string>fetch</string>
    <string>remote-notification</string>
</array>
```

---

## Часть 3: Запуск на iPhone через кабель (БЕСПЛАТНО)

### 3.1 Подключи iPhone к Mac
- Используй Lightning/USB-C кабель
- На iPhone нажми **"Доверять этому компьютеру"**

### 3.2 Открой проект в Xcode
```bash
open ios/Runner.xcworkspace
```

### 3.3 Настрой подпись (Signing)
1. В Xcode слева нажми на **Runner** (синяя иконка проекта)
2. Вкладка **Signing & Capabilities**
3. Поставь галочку **"Automatically manage signing"**
4. В **Team** нажми **"Add an Account..."**
5. Войди со своим **Apple ID** (обычный, бесплатный)
6. Выбери свой аккаунт как Team
7. Если Xcode ругается на Bundle Identifier — измени его на уникальный,
   например: `com.твоёимя.unischedule`

### 3.4 Выбери устройство
- Вверху Xcode, рядом с кнопкой Play, выбери свой iPhone (не симулятор!)

### 3.5 Запусти
- Нажми **Play** (▶) или `Cmd+R`
- Первый раз iPhone попросит: **Настройки → Основные → Управление устройством →
  нажми на свой Apple ID → "Доверять"**
- Приложение установится на iPhone!

### Альтернатива — через терминал:
```bash
flutter devices                    # посмотри ID своего iPhone
flutter run -d <твой_device_id>    # запусти
```

> **Ограничение бесплатного аккаунта**: приложение работает 7 дней.
> После этого нужно снова подключить к Mac и нажать Play.
> Для постоянной работы нужен Apple Developer ($99/год).

---

## Часть 4: Публикация в App Store (нужен Apple Developer)

### 4.1 Зарегистрируйся в Apple Developer Program
- Перейди на https://developer.apple.com/programs/
- Нажми **Enroll**
- Стоимость: **$99/год** (~9000 руб)
- Регистрация занимает 1-2 дня (Apple проверяет)

### 4.2 Создай приложение в App Store Connect
1. Зайди на https://appstoreconnect.apple.com
2. **Мои приложения** → **+** → **Новое приложение**
3. Заполни:
   - Название: **UniSchedule**
   - Язык: **Русский**
   - Bundle ID: выбери тот, что в Xcode
   - SKU: `unischedule001`

### 4.3 Собери релизную версию
```bash
flutter build ipa --release
```
Файл `.ipa` появится в `build/ios/ipa/`

### 4.4 Загрузи в App Store Connect
- Скачай приложение **Transporter** из Mac App Store (бесплатно)
- Открой Transporter, перетащи туда файл `.ipa`
- Нажми **Доставить**

### 4.5 Отправь на ревью
1. Вернись в App Store Connect
2. Выбери загруженную сборку
3. Заполни описание, скриншоты, возрастной рейтинг
4. Нажми **Отправить на проверку**
5. Apple проверяет 1-3 дня

### 4.6 Скриншоты
Нужны скриншоты для:
- iPhone 6.7" (iPhone 15 Pro Max) — обязательно
- iPhone 6.5" (iPhone 11 Pro Max) — обязательно
- iPad (если поддерживаешь)

Сделать скриншоты можно на симуляторе:
```bash
flutter run                          # запусти на симуляторе
# В симуляторе: Cmd+S сохраняет скриншот на рабочий стол
```

---

## Часть 5 (альтернатива): TestFlight — бета-тестирование

Если не хочешь сразу в App Store, но хочешь раздать друзьям/группе:

1. Загрузи `.ipa` через Transporter (как в 4.4)
2. В App Store Connect → **TestFlight**
3. Добавь email-адреса тестировщиков (до 10 000 человек)
4. Им придёт приглашение — они скачают через приложение TestFlight
5. Проверка Apple для TestFlight обычно 1 день

---

## FAQ

**Q: Мне точно нужно платить $99?**
Нет, если хочешь только себе на iPhone — достаточно бесплатного Apple ID.
$99 нужен только для App Store / TestFlight.

**Q: Могу ли я тестировать без iPhone?**
Да! Xcode включает симулятор iPhone. `flutter run` запустит в нём.

**Q: Что если flutter doctor показывает ошибки?**
Выполни то, что он предлагает. Обычно это:
```bash
sudo xcode-select --install
sudo gem install cocoapods
flutter doctor --android-licenses   # если нужен Android
```

**Q: А для Android?**
Установи Android Studio, и `flutter build apk` создаст APK-файл.
Его можно скинуть на любой Android-телефон и установить.
