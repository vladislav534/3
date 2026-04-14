import 'package:hive/hive.dart';

part 'lesson.g.dart';

enum LessonType {
  lecture,
  practice,
  lab,
  seminar,
  exam,
  consultation,
}

enum WeekType {
  both,
  upper,
  lower,
}

extension LessonTypeExtension on LessonType {
  String get displayName {
    switch (this) {
      case LessonType.lecture:
        return 'Лекция';
      case LessonType.practice:
        return 'Практика';
      case LessonType.lab:
        return 'Лабораторная';
      case LessonType.seminar:
        return 'Семинар';
      case LessonType.exam:
        return 'Экзамен';
      case LessonType.consultation:
        return 'Консультация';
    }
  }

  String get shortName {
    switch (this) {
      case LessonType.lecture:
        return 'ЛК';
      case LessonType.practice:
        return 'ПР';
      case LessonType.lab:
        return 'ЛАБ';
      case LessonType.seminar:
        return 'СЕМ';
      case LessonType.exam:
        return 'ЭКЗ';
      case LessonType.consultation:
        return 'КОНС';
    }
  }
}

extension WeekTypeExtension on WeekType {
  String get displayName {
    switch (this) {
      case WeekType.both:
        return 'Каждую неделю';
      case WeekType.upper:
        return 'Верхняя неделя';
      case WeekType.lower:
        return 'Нижняя неделя';
    }
  }
}

@HiveType(typeId: 0)
class Lesson extends HiveObject {
  @HiveField(0)
  final String id;

  @HiveField(1)
  final String subject;

  @HiveField(2)
  final String teacher;

  @HiveField(3)
  final String room;

  @HiveField(4)
  final int lessonTypeIndex;

  @HiveField(5)
  final String startTime; // "08:30"

  @HiveField(6)
  final String endTime; // "10:05"

  @HiveField(7)
  final int dayOfWeek; // 1 = Monday, 7 = Sunday

  @HiveField(8)
  final int weekTypeIndex;

  @HiveField(9)
  final int pairNumber; // 1-based pair number

  @HiveField(10)
  final String? subgroup; // null = all, "1", "2"

  Lesson({
    required this.id,
    required this.subject,
    required this.teacher,
    required this.room,
    required this.lessonTypeIndex,
    required this.startTime,
    required this.endTime,
    required this.dayOfWeek,
    required this.weekTypeIndex,
    required this.pairNumber,
    this.subgroup,
  });

  LessonType get lessonType => LessonType.values[lessonTypeIndex];
  WeekType get weekType => WeekType.values[weekTypeIndex];

  DateTime get startDateTime {
    final parts = startTime.split(':');
    final now = DateTime.now();
    return DateTime(now.year, now.month, now.day,
        int.parse(parts[0]), int.parse(parts[1]));
  }

  DateTime get endDateTime {
    final parts = endTime.split(':');
    final now = DateTime.now();
    return DateTime(now.year, now.month, now.day,
        int.parse(parts[0]), int.parse(parts[1]));
  }

  Lesson copyWith({
    String? id,
    String? subject,
    String? teacher,
    String? room,
    int? lessonTypeIndex,
    String? startTime,
    String? endTime,
    int? dayOfWeek,
    int? weekTypeIndex,
    int? pairNumber,
    String? subgroup,
  }) {
    return Lesson(
      id: id ?? this.id,
      subject: subject ?? this.subject,
      teacher: teacher ?? this.teacher,
      room: room ?? this.room,
      lessonTypeIndex: lessonTypeIndex ?? this.lessonTypeIndex,
      startTime: startTime ?? this.startTime,
      endTime: endTime ?? this.endTime,
      dayOfWeek: dayOfWeek ?? this.dayOfWeek,
      weekTypeIndex: weekTypeIndex ?? this.weekTypeIndex,
      pairNumber: pairNumber ?? this.pairNumber,
      subgroup: subgroup ?? this.subgroup,
    );
  }

  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'subject': subject,
      'teacher': teacher,
      'room': room,
      'lessonTypeIndex': lessonTypeIndex,
      'startTime': startTime,
      'endTime': endTime,
      'dayOfWeek': dayOfWeek,
      'weekTypeIndex': weekTypeIndex,
      'pairNumber': pairNumber,
      'subgroup': subgroup,
    };
  }

  factory Lesson.fromJson(Map<String, dynamic> json) {
    return Lesson(
      id: json['id'] as String,
      subject: json['subject'] as String,
      teacher: json['teacher'] as String,
      room: json['room'] as String,
      lessonTypeIndex: json['lessonTypeIndex'] as int,
      startTime: json['startTime'] as String,
      endTime: json['endTime'] as String,
      dayOfWeek: json['dayOfWeek'] as int,
      weekTypeIndex: json['weekTypeIndex'] as int,
      pairNumber: json['pairNumber'] as int,
      subgroup: json['subgroup'] as String?,
    );
  }
}
