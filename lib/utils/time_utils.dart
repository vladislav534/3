class TimeUtils {
  /// Форматирует разницу во времени в человекочитаемый формат
  static String formatDuration(Duration duration) {
    if (duration.isNegative) return 'Уже прошла';

    final days = duration.inDays;
    final hours = duration.inHours % 24;
    final minutes = duration.inMinutes % 60;

    if (days > 0) {
      return '$days ${_pluralDays(days)} $hours ${_pluralHours(hours)}';
    } else if (hours > 0) {
      return '$hours ${_pluralHours(hours)} $minutes ${_pluralMinutes(minutes)}';
    } else {
      return '$minutes ${_pluralMinutes(minutes)}';
    }
  }

  /// Форматирует время до следующей пары
  static String formatTimeUntilLesson(DateTime lessonStart) {
    final now = DateTime.now();
    final diff = lessonStart.difference(now);
    return formatDuration(diff);
  }

  /// Вычисляет дату следующего вхождения дня недели
  static DateTime getNextDateForWeekday(int targetWeekday, [DateTime? from]) {
    final now = from ?? DateTime.now();
    int daysUntil = targetWeekday - now.weekday;
    if (daysUntil <= 0) daysUntil += 7;
    return DateTime(now.year, now.month, now.day + daysUntil);
  }

  /// Парсит строку времени "HH:MM" в DateTime сегодня
  static DateTime parseTimeToday(String time) {
    final parts = time.split(':');
    final now = DateTime.now();
    return DateTime(
      now.year, now.month, now.day,
      int.parse(parts[0]), int.parse(parts[1]),
    );
  }

  /// Проверяет, идёт ли сейчас эта пара
  static bool isLessonActive(String startTime, String endTime) {
    final now = DateTime.now();
    final start = parseTimeToday(startTime);
    final end = parseTimeToday(endTime);
    return now.isAfter(start) && now.isBefore(end);
  }

  /// Проверяет, пара уже прошла сегодня
  static bool isLessonPassed(String endTime) {
    final now = DateTime.now();
    final end = parseTimeToday(endTime);
    return now.isAfter(end);
  }

  // --- Plural helpers for Russian ---

  static String _pluralDays(int n) {
    if (n % 10 == 1 && n % 100 != 11) return 'день';
    if (n % 10 >= 2 && n % 10 <= 4 && (n % 100 < 10 || n % 100 >= 20)) {
      return 'дня';
    }
    return 'дней';
  }

  static String _pluralHours(int n) {
    if (n % 10 == 1 && n % 100 != 11) return 'час';
    if (n % 10 >= 2 && n % 10 <= 4 && (n % 100 < 10 || n % 100 >= 20)) {
      return 'часа';
    }
    return 'часов';
  }

  static String _pluralMinutes(int n) {
    if (n % 10 == 1 && n % 100 != 11) return 'минута';
    if (n % 10 >= 2 && n % 10 <= 4 && (n % 100 < 10 || n % 100 >= 20)) {
      return 'минуты';
    }
    return 'минут';
  }
}
