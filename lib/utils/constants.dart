class AppConstants {
  static const String appName = 'UniSchedule';
  static const String appVersion = '1.0.0';

  // Admin credentials (in production, use Firebase Auth)
  static const String adminEmail = 'admin@unischedule.ru';
  static const String adminPassword = 'admin123';

  // Day names
  static const List<String> dayNames = [
    'Понедельник',
    'Вторник',
    'Среда',
    'Четверг',
    'Пятница',
    'Суббота',
    'Воскресенье',
  ];

  static const List<String> dayNamesShort = [
    'ПН',
    'ВТ',
    'СР',
    'ЧТ',
    'ПТ',
    'СБ',
    'ВС',
  ];

  // Course options
  static const int minCourse = 1;
  static const int maxCourse = 6;

  // Pair numbers
  static const int maxPairs = 7;

  static String getDayName(int dayOfWeek) {
    if (dayOfWeek < 1 || dayOfWeek > 7) return '';
    return dayNames[dayOfWeek - 1];
  }

  static String getDayNameShort(int dayOfWeek) {
    if (dayOfWeek < 1 || dayOfWeek > 7) return '';
    return dayNamesShort[dayOfWeek - 1];
  }
}
