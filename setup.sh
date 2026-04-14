#!/bin/bash
# =============================================================
# UniSchedule — скрипт быстрой настройки проекта
# Запусти на Mac после клонирования репозитория
# =============================================================

set -e

echo "=== UniSchedule: Настройка проекта ==="
echo ""

# 1. Check Flutter
if ! command -v flutter &> /dev/null; then
    echo "Flutter не найден. Установи через: brew install flutter"
    echo "Или скачай с https://docs.flutter.dev/get-started/install/macos"
    exit 1
fi

echo "[1/5] Flutter найден: $(flutter --version | head -1)"

# 2. Check Xcode
if ! command -v xcodebuild &> /dev/null; then
    echo "Xcode не найден. Установи через App Store."
    exit 1
fi
echo "[2/5] Xcode найден"

# 3. Generate platform files
echo "[3/5] Генерация iOS/Android файлов..."
flutter create --project-name uni_schedule --org com.unischedule .

# 4. Install dependencies
echo "[4/5] Установка зависимостей..."
flutter pub get

# 5. iOS-specific setup
echo "[5/5] Настройка iOS..."
cd ios
pod install
cd ..

echo ""
echo "=== Готово! ==="
echo ""
echo "Запуск на iOS-симуляторе:"
echo "  flutter run"
echo ""
echo "Запуск на подключённом iPhone:"
echo "  flutter run -d <device_id>"
echo "  (узнать device_id: flutter devices)"
echo ""
echo "Сборка .ipa для TestFlight:"
echo "  flutter build ipa"
echo ""
