import 'lesson.dart';

class Schedule {
  final String group;
  final int course;
  final List<Lesson> lessons;

  Schedule({
    required this.group,
    required this.course,
    required this.lessons,
  });

  List<Lesson> getLessonsForDay(int dayOfWeek, WeekType currentWeekType) {
    return lessons.where((lesson) {
      if (lesson.dayOfWeek != dayOfWeek) return false;
      if (lesson.weekType == WeekType.both) return true;
      return lesson.weekType == currentWeekType;
    }).toList()
      ..sort((a, b) => a.pairNumber.compareTo(b.pairNumber));
  }

  Lesson? getNextLesson(DateTime now, WeekType currentWeekType) {
    final today = now.weekday;
    final currentTime = '${now.hour.toString().padLeft(2, '0')}:${now.minute.toString().padLeft(2, '0')}';

    // Check today's remaining lessons
    final todayLessons = getLessonsForDay(today, currentWeekType);
    for (final lesson in todayLessons) {
      if (lesson.endTime.compareTo(currentTime) > 0) {
        return lesson;
      }
    }

    // Check next days (up to 7 days ahead)
    for (int i = 1; i <= 7; i++) {
      final nextDay = ((today - 1 + i) % 7) + 1;
      final nextDayLessons = getLessonsForDay(nextDay, currentWeekType);
      if (nextDayLessons.isNotEmpty) {
        return nextDayLessons.first;
      }
    }

    return null;
  }

  Lesson? getNextLessonForSubject(
    String subject,
    int currentDayOfWeek,
    int currentPairNumber,
    WeekType currentWeekType,
  ) {
    final subjectLessons = lessons.where((l) => l.subject == subject).toList();

    // Find next occurrence after current
    for (int dayOffset = 0; dayOffset <= 14; dayOffset++) {
      final checkDay = ((currentDayOfWeek - 1 + dayOffset) % 7) + 1;
      final isNextWeek = dayOffset >= 7;
      final checkWeekType = isNextWeek
          ? (currentWeekType == WeekType.upper ? WeekType.lower : WeekType.upper)
          : currentWeekType;

      for (final lesson in subjectLessons) {
        if (lesson.dayOfWeek != checkDay) continue;
        if (lesson.weekType != WeekType.both && lesson.weekType != checkWeekType) {
          continue;
        }

        // Skip current lesson
        if (dayOffset == 0 && lesson.pairNumber <= currentPairNumber) continue;

        return lesson;
      }
    }

    return null;
  }

  Map<String, dynamic> toJson() {
    return {
      'group': group,
      'course': course,
      'lessons': lessons.map((l) => l.toJson()).toList(),
    };
  }

  factory Schedule.fromJson(Map<String, dynamic> json) {
    return Schedule(
      group: json['group'] as String,
      course: json['course'] as int,
      lessons: (json['lessons'] as List)
          .map((l) => Lesson.fromJson(l as Map<String, dynamic>))
          .toList(),
    );
  }
}
