import 'package:uuid/uuid.dart';
import '../models/lesson.dart';
import '../models/schedule.dart';

class ScheduleService {
  static const _uuid = Uuid();

  /// Стандартное расписание звонков
  static const List<Map<String, String>> bellSchedule = [
    {'start': '08:30', 'end': '10:05'},
    {'start': '10:15', 'end': '11:50'},
    {'start': '12:00', 'end': '13:35'},
    {'start': '14:15', 'end': '15:50'},
    {'start': '16:00', 'end': '17:35'},
    {'start': '17:45', 'end': '19:20'},
    {'start': '19:30', 'end': '21:05'},
  ];

  static String getPairTime(int pairNumber) {
    if (pairNumber < 1 || pairNumber > bellSchedule.length) {
      return '';
    }
    final pair = bellSchedule[pairNumber - 1];
    return '${pair['start']} — ${pair['end']}';
  }

  /// Генерация демо-расписания для тестирования
  static Schedule generateDemoSchedule(String group, int course) {
    final lessons = <Lesson>[];

    // Понедельник
    lessons.addAll([
      _createLesson('Математический анализ', 'Иванов И.И.',
          '301', LessonType.lecture, 1, 1, WeekType.both),
      _createLesson('Программирование', 'Петров П.П.',
          '412', LessonType.practice, 1, 2, WeekType.both),
      _createLesson('Физика', 'Сидоров С.С.',
          '205', LessonType.lecture, 1, 3, WeekType.upper),
      _createLesson('Английский язык', 'Козлова А.В.',
          '110', LessonType.practice, 1, 3, WeekType.lower),
    ]);

    // Вторник
    lessons.addAll([
      _createLesson('Программирование', 'Петров П.П.',
          '415', LessonType.lab, 2, 1, WeekType.both),
      _createLesson('Линейная алгебра', 'Смирнова О.Н.',
          '302', LessonType.lecture, 2, 2, WeekType.both),
      _createLesson('История', 'Кузнецов Д.А.',
          '101', LessonType.seminar, 2, 3, WeekType.upper),
    ]);

    // Среда
    lessons.addAll([
      _createLesson('Физика', 'Сидоров С.С.',
          '207', LessonType.lab, 3, 1, WeekType.upper),
      _createLesson('Математический анализ', 'Иванов И.И.',
          '303', LessonType.practice, 3, 2, WeekType.both),
      _createLesson('Дискретная математика', 'Волкова Е.М.',
          '304', LessonType.lecture, 3, 3, WeekType.both),
      _createLesson('Программирование', 'Петров П.П.',
          '412', LessonType.lecture, 3, 4, WeekType.lower),
    ]);

    // Четверг
    lessons.addAll([
      _createLesson('Английский язык', 'Козлова А.В.',
          '110', LessonType.practice, 4, 1, WeekType.both),
      _createLesson('Физика', 'Сидоров С.С.',
          '205', LessonType.practice, 4, 2, WeekType.both),
      _createLesson('Линейная алгебра', 'Смирнова О.Н.',
          '302', LessonType.practice, 4, 3, WeekType.upper),
    ]);

    // Пятница
    lessons.addAll([
      _createLesson('Дискретная математика', 'Волкова Е.М.',
          '304', LessonType.practice, 5, 1, WeekType.both),
      _createLesson('Математический анализ', 'Иванов И.И.',
          '301', LessonType.lecture, 5, 2, WeekType.lower),
      _createLesson('Физкультура', 'Борисов К.Л.',
          'СК', LessonType.practice, 5, 3, WeekType.both),
    ]);

    // Суббота
    lessons.addAll([
      _createLesson('Линейная алгебра', 'Смирнова О.Н.',
          '302', LessonType.lecture, 6, 1, WeekType.upper),
      _createLesson('История', 'Кузнецов Д.А.',
          '101', LessonType.lecture, 6, 2, WeekType.upper),
    ]);

    return Schedule(
      group: group,
      course: course,
      lessons: lessons,
    );
  }

  static Lesson _createLesson(
    String subject,
    String teacher,
    String room,
    LessonType type,
    int dayOfWeek,
    int pairNumber,
    WeekType weekType,
  ) {
    final times = bellSchedule[pairNumber - 1];
    return Lesson(
      id: _uuid.v4(),
      subject: subject,
      teacher: teacher,
      room: room,
      lessonTypeIndex: type.index,
      startTime: times['start']!,
      endTime: times['end']!,
      dayOfWeek: dayOfWeek,
      weekTypeIndex: weekType.index,
      pairNumber: pairNumber,
    );
  }

  /// Создать новую пару
  static Lesson createLesson({
    required String subject,
    required String teacher,
    required String room,
    required LessonType type,
    required int dayOfWeek,
    required int pairNumber,
    required WeekType weekType,
    String? subgroup,
  }) {
    final times = bellSchedule[pairNumber - 1];
    return Lesson(
      id: _uuid.v4(),
      subject: subject,
      teacher: teacher,
      room: room,
      lessonTypeIndex: type.index,
      startTime: times['start']!,
      endTime: times['end']!,
      dayOfWeek: dayOfWeek,
      weekTypeIndex: weekType.index,
      pairNumber: pairNumber,
      subgroup: subgroup,
    );
  }
}
