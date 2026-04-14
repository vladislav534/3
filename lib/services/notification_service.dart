import 'package:flutter_local_notifications/flutter_local_notifications.dart';
import 'package:timezone/timezone.dart' as tz;
import 'package:timezone/data/latest.dart' as tz_data;
import '../models/lesson.dart';

class NotificationService {
  static final NotificationService _instance = NotificationService._();
  factory NotificationService() => _instance;
  NotificationService._();

  final FlutterLocalNotificationsPlugin _notifications =
      FlutterLocalNotificationsPlugin();

  Future<void> init() async {
    tz_data.initializeTimeZones();

    const androidSettings = AndroidInitializationSettings(
      '@mipmap/ic_launcher',
    );

    const iosSettings = DarwinInitializationSettings(
      requestAlertPermission: true,
      requestBadgePermission: true,
      requestSoundPermission: true,
    );

    const settings = InitializationSettings(
      android: androidSettings,
      iOS: iosSettings,
    );

    await _notifications.initialize(
      settings,
      onDidReceiveNotificationResponse: _onNotificationTap,
    );
  }

  void _onNotificationTap(NotificationResponse response) {
    // Handle notification tap — navigate to lesson detail
    print('Notification tapped: ${response.payload}');
  }

  Future<void> requestPermissions() async {
    await _notifications
        .resolvePlatformSpecificImplementation<
            AndroidFlutterLocalNotificationsPlugin>()
        ?.requestNotificationsPermission();

    await _notifications
        .resolvePlatformSpecificImplementation<
            IOSFlutterLocalNotificationsPlugin>()
        ?.requestPermissions(
          alert: true,
          badge: true,
          sound: true,
        );
  }

  /// Schedule a reminder before a lesson
  Future<void> scheduleLessonReminder({
    required Lesson lesson,
    required DateTime lessonDateTime,
    int minutesBefore = 15,
  }) async {
    final scheduledDate = lessonDateTime.subtract(
      Duration(minutes: minutesBefore),
    );

    if (scheduledDate.isBefore(DateTime.now())) return;

    final id = lesson.id.hashCode.abs() % 100000;

    await _notifications.zonedSchedule(
      id,
      'Скоро пара: ${lesson.subject}',
      '${lesson.lessonType.displayName} в ауд. ${lesson.room} через $minutesBefore мин.',
      tz.TZDateTime.from(scheduledDate, tz.local),
      NotificationDetails(
        android: AndroidNotificationDetails(
          'lesson_reminders',
          'Напоминания о парах',
          channelDescription: 'Уведомления перед началом пар',
          importance: Importance.high,
          priority: Priority.high,
          icon: '@mipmap/ic_launcher',
        ),
        iOS: const DarwinNotificationDetails(
          presentAlert: true,
          presentBadge: true,
          presentSound: true,
        ),
      ),
      androidScheduleMode: AndroidScheduleMode.exactAllowWhileIdle,
      payload: lesson.id,
    );
  }

  /// Schedule a custom reminder with note
  Future<void> scheduleNoteReminder({
    required String noteId,
    required String subject,
    required String noteText,
    required DateTime reminderDate,
  }) async {
    final id = noteId.hashCode.abs() % 100000;

    await _notifications.zonedSchedule(
      id,
      'Заметка: $subject',
      noteText.length > 100 ? '${noteText.substring(0, 100)}...' : noteText,
      tz.TZDateTime.from(reminderDate, tz.local),
      NotificationDetails(
        android: AndroidNotificationDetails(
          'note_reminders',
          'Напоминания по заметкам',
          channelDescription: 'Уведомления по заметкам к парам',
          importance: Importance.high,
          priority: Priority.high,
          icon: '@mipmap/ic_launcher',
        ),
        iOS: const DarwinNotificationDetails(
          presentAlert: true,
          presentBadge: true,
          presentSound: true,
        ),
      ),
      androidScheduleMode: AndroidScheduleMode.exactAllowWhileIdle,
      payload: 'note_$noteId',
    );
  }

  /// Cancel a specific notification
  Future<void> cancelNotification(String id) async {
    await _notifications.cancel(id.hashCode.abs() % 100000);
  }

  /// Cancel all notifications
  Future<void> cancelAll() async {
    await _notifications.cancelAll();
  }
}
