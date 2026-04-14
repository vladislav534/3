import 'package:flutter/foundation.dart';
import 'package:uuid/uuid.dart';
import '../models/lesson.dart';
import '../models/note.dart';
import '../models/schedule.dart';
import '../services/storage_service.dart';
import '../services/schedule_service.dart';
import '../services/week_type_service.dart';
import '../services/notification_service.dart';

class AppProvider extends ChangeNotifier {
  final StorageService _storage;
  final NotificationService _notifications;
  late WeekTypeService _weekTypeService;

  static const _uuid = Uuid();

  // State
  int? _course;
  String? _group;
  bool _isAdmin = false;
  bool _onboardingComplete = false;
  Schedule? _schedule;
  List<Note> _notes = [];
  int _selectedDayOfWeek = DateTime.now().weekday;

  // Getters
  int? get course => _course;
  String? get group => _group;
  bool get isAdmin => _isAdmin;
  bool get onboardingComplete => _onboardingComplete;
  Schedule? get schedule => _schedule;
  List<Note> get notes => _notes;
  int get selectedDayOfWeek => _selectedDayOfWeek;
  WeekType get currentWeekType => _weekTypeService.getCurrentWeekType();
  String get currentWeekDisplayName => _weekTypeService.getCurrentWeekDisplayName();
  int get currentWeekNumber => _weekTypeService.getWeekNumber();

  AppProvider({
    required StorageService storage,
    required NotificationService notifications,
  })  : _storage = storage,
        _notifications = notifications {
    _weekTypeService = WeekTypeService.defaultSemester();
    _loadState();
  }

  void _loadState() {
    _course = _storage.getCourse();
    _group = _storage.getGroup();
    _isAdmin = _storage.isAdmin();
    _onboardingComplete = _storage.isOnboardingComplete();

    final semesterStart = _storage.getSemesterStart();
    if (semesterStart != null) {
      _weekTypeService = WeekTypeService(
        semesterStart: semesterStart,
        firstWeekType: _storage.getFirstWeekType() == 0
            ? WeekType.upper
            : WeekType.lower,
      );
    }

    // Load schedule from local storage
    final lessons = _storage.getAllLessons();
    if (lessons.isNotEmpty && _group != null && _course != null) {
      _schedule = Schedule(
        group: _group!,
        course: _course!,
        lessons: lessons,
      );
    }

    _notes = _storage.getAllNotes();
    notifyListeners();
  }

  // --- Onboarding ---

  Future<void> completeOnboarding({
    required int course,
    required String group,
  }) async {
    _course = course;
    _group = group;
    _onboardingComplete = true;

    await _storage.setCourse(course);
    await _storage.setGroup(group);
    await _storage.setOnboardingComplete(true);

    // Load demo schedule
    final demoSchedule = ScheduleService.generateDemoSchedule(group, course);
    await _storage.importLessons(demoSchedule.lessons);
    _schedule = demoSchedule;

    notifyListeners();
  }

  // --- Day Selection ---

  void selectDay(int dayOfWeek) {
    if (dayOfWeek >= 1 && dayOfWeek <= 7) {
      _selectedDayOfWeek = dayOfWeek;
      notifyListeners();
    }
  }

  // --- Schedule ---

  List<Lesson> get todayLessons {
    if (_schedule == null) return [];
    return _schedule!.getLessonsForDay(
      _selectedDayOfWeek,
      currentWeekType,
    );
  }

  Lesson? get nextLesson {
    if (_schedule == null) return null;
    return _schedule!.getNextLesson(DateTime.now(), currentWeekType);
  }

  Lesson? getNextLessonForSubject(String subject, int dayOfWeek, int pairNumber) {
    if (_schedule == null) return null;
    return _schedule!.getNextLessonForSubject(
      subject, dayOfWeek, pairNumber, currentWeekType,
    );
  }

  Future<void> addLesson(Lesson lesson) async {
    await _storage.addLesson(lesson);
    _reloadSchedule();
    notifyListeners();
  }

  Future<void> updateLesson(Lesson lesson) async {
    await _storage.updateLesson(lesson);
    _reloadSchedule();
    notifyListeners();
  }

  Future<void> deleteLesson(String id) async {
    await _storage.deleteLesson(id);
    _reloadSchedule();
    notifyListeners();
  }

  Future<void> importSchedule(List<Lesson> lessons) async {
    await _storage.importLessons(lessons);
    _reloadSchedule();
    notifyListeners();
  }

  void _reloadSchedule() {
    final lessons = _storage.getAllLessons();
    if (_group != null && _course != null) {
      _schedule = Schedule(
        group: _group!,
        course: _course!,
        lessons: lessons,
      );
    }
  }

  // --- Notes ---

  List<Note> getNotesForSubject(String subject) {
    return _notes.where((n) => n.subject == subject).toList();
  }

  Future<void> addNote({
    required String lessonId,
    required String subject,
    required String text,
    DateTime? reminderAt,
  }) async {
    final note = Note(
      id: _uuid.v4(),
      lessonId: lessonId,
      subject: subject,
      text: text,
      createdAt: DateTime.now(),
      reminderAt: reminderAt,
    );

    await _storage.addNote(note);
    _notes = _storage.getAllNotes();

    if (reminderAt != null) {
      await _notifications.scheduleNoteReminder(
        noteId: note.id,
        subject: subject,
        noteText: text,
        reminderDate: reminderAt,
      );
    }

    notifyListeners();
  }

  Future<void> deleteNote(String id) async {
    await _storage.deleteNote(id);
    await _notifications.cancelNotification(id);
    _notes = _storage.getAllNotes();
    notifyListeners();
  }

  // --- Admin ---

  Future<bool> loginAsAdmin(String email, String password) async {
    // In production, use Firebase Auth
    if (email == 'admin@unischedule.ru' && password == 'admin123') {
      _isAdmin = true;
      await _storage.setAdmin(true);
      notifyListeners();
      return true;
    }
    return false;
  }

  Future<void> logoutAdmin() async {
    _isAdmin = false;
    await _storage.setAdmin(false);
    notifyListeners();
  }

  // --- Settings ---

  Future<void> updateGroup(String group) async {
    _group = group;
    await _storage.setGroup(group);
    notifyListeners();
  }

  Future<void> updateCourse(int course) async {
    _course = course;
    await _storage.setCourse(course);
    notifyListeners();
  }

  Future<void> resetAll() async {
    await _storage.resetAll();
    _course = null;
    _group = null;
    _isAdmin = false;
    _onboardingComplete = false;
    _schedule = null;
    _notes = [];
    notifyListeners();
  }
}
