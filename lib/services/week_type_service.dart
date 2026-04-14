import '../models/lesson.dart';

class WeekTypeService {
  final DateTime semesterStart;
  final WeekType firstWeekType;

  WeekTypeService({
    required this.semesterStart,
    this.firstWeekType = WeekType.upper,
  });

  /// Default semester start: September 1, 2025
  factory WeekTypeService.defaultSemester() {
    return WeekTypeService(
      semesterStart: DateTime(2025, 9, 1),
      firstWeekType: WeekType.upper,
    );
  }

  /// Определяет тип текущей недели (верхняя/нижняя)
  WeekType getCurrentWeekType([DateTime? date]) {
    final now = date ?? DateTime.now();

    // Find the Monday of the semester start week
    final semesterMonday = semesterStart.subtract(
      Duration(days: semesterStart.weekday - 1),
    );

    // Find the Monday of the current week
    final currentMonday = now.subtract(
      Duration(days: now.weekday - 1),
    );

    final daysDiff = currentMonday.difference(semesterMonday).inDays;
    final weekNumber = (daysDiff / 7).floor();

    // Even weeks = first week type, odd weeks = opposite
    if (weekNumber.isEven) {
      return firstWeekType;
    } else {
      return firstWeekType == WeekType.upper
          ? WeekType.lower
          : WeekType.upper;
    }
  }

  /// Номер учебной недели (начиная с 1)
  int getWeekNumber([DateTime? date]) {
    final now = date ?? DateTime.now();
    final semesterMonday = semesterStart.subtract(
      Duration(days: semesterStart.weekday - 1),
    );
    final currentMonday = now.subtract(
      Duration(days: now.weekday - 1),
    );
    final daysDiff = currentMonday.difference(semesterMonday).inDays;
    return (daysDiff / 7).floor() + 1;
  }

  /// Отображаемое название текущей недели
  String getCurrentWeekDisplayName([DateTime? date]) {
    final weekType = getCurrentWeekType(date);
    final weekNum = getWeekNumber(date);
    return '${weekType.displayName} (неделя $weekNum)';
  }
}
