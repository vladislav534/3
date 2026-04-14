import 'package:hive_flutter/hive_flutter.dart';
import 'package:shared_preferences/shared_preferences.dart';
import '../models/lesson.dart';
import '../models/note.dart';

class StorageService {
  static const String _lessonsBoxName = 'lessons';
  static const String _notesBoxName = 'notes';
  static const String _courseKey = 'user_course';
  static const String _groupKey = 'user_group';
  static const String _isAdminKey = 'is_admin';
  static const String _onboardingCompleteKey = 'onboarding_complete';
  static const String _semesterStartKey = 'semester_start';
  static const String _firstWeekTypeKey = 'first_week_type';

  late Box<Lesson> _lessonsBox;
  late Box<Note> _notesBox;
  late SharedPreferences _prefs;

  Future<void> init() async {
    await Hive.initFlutter();

    Hive.registerAdapter(LessonAdapter());
    Hive.registerAdapter(NoteAdapter());

    _lessonsBox = await Hive.openBox<Lesson>(_lessonsBoxName);
    _notesBox = await Hive.openBox<Note>(_notesBoxName);
    _prefs = await SharedPreferences.getInstance();
  }

  // --- User Settings ---

  int? getCourse() => _prefs.getInt(_courseKey);
  Future<void> setCourse(int course) => _prefs.setInt(_courseKey, course);

  String? getGroup() => _prefs.getString(_groupKey);
  Future<void> setGroup(String group) => _prefs.setString(_groupKey, group);

  bool isAdmin() => _prefs.getBool(_isAdminKey) ?? false;
  Future<void> setAdmin(bool value) => _prefs.setBool(_isAdminKey, value);

  bool isOnboardingComplete() =>
      _prefs.getBool(_onboardingCompleteKey) ?? false;
  Future<void> setOnboardingComplete(bool value) =>
      _prefs.setBool(_onboardingCompleteKey, value);

  // --- Semester Settings ---

  DateTime? getSemesterStart() {
    final str = _prefs.getString(_semesterStartKey);
    return str != null ? DateTime.parse(str) : null;
  }

  Future<void> setSemesterStart(DateTime date) =>
      _prefs.setString(_semesterStartKey, date.toIso8601String());

  /// 0 = upper, 1 = lower — тип первой недели семестра
  int getFirstWeekType() => _prefs.getInt(_firstWeekTypeKey) ?? 0;
  Future<void> setFirstWeekType(int type) =>
      _prefs.setInt(_firstWeekTypeKey, type);

  // --- Lessons ---

  List<Lesson> getAllLessons() => _lessonsBox.values.toList();

  Future<void> addLesson(Lesson lesson) =>
      _lessonsBox.put(lesson.id, lesson);

  Future<void> updateLesson(Lesson lesson) =>
      _lessonsBox.put(lesson.id, lesson);

  Future<void> deleteLesson(String id) => _lessonsBox.delete(id);

  Future<void> clearAllLessons() => _lessonsBox.clear();

  Future<void> importLessons(List<Lesson> lessons) async {
    await _lessonsBox.clear();
    for (final lesson in lessons) {
      await _lessonsBox.put(lesson.id, lesson);
    }
  }

  // --- Notes ---

  List<Note> getAllNotes() => _notesBox.values.toList();

  List<Note> getNotesForLesson(String lessonId) =>
      _notesBox.values.where((n) => n.lessonId == lessonId).toList();

  List<Note> getNotesForSubject(String subject) =>
      _notesBox.values.where((n) => n.subject == subject).toList();

  Future<void> addNote(Note note) => _notesBox.put(note.id, note);

  Future<void> updateNote(Note note) => _notesBox.put(note.id, note);

  Future<void> deleteNote(String id) => _notesBox.delete(id);

  // --- Reset ---

  Future<void> resetAll() async {
    await _lessonsBox.clear();
    await _notesBox.clear();
    await _prefs.clear();
  }
}
