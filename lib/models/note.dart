import 'package:hive/hive.dart';

part 'note.g.dart';

@HiveType(typeId: 1)
class Note extends HiveObject {
  @HiveField(0)
  final String id;

  @HiveField(1)
  final String lessonId;

  @HiveField(2)
  final String subject;

  @HiveField(3)
  String text;

  @HiveField(4)
  final DateTime createdAt;

  @HiveField(5)
  DateTime? reminderAt;

  Note({
    required this.id,
    required this.lessonId,
    required this.subject,
    required this.text,
    required this.createdAt,
    this.reminderAt,
  });

  Map<String, dynamic> toJson() {
    return {
      'id': id,
      'lessonId': lessonId,
      'subject': subject,
      'text': text,
      'createdAt': createdAt.toIso8601String(),
      'reminderAt': reminderAt?.toIso8601String(),
    };
  }

  factory Note.fromJson(Map<String, dynamic> json) {
    return Note(
      id: json['id'] as String,
      lessonId: json['lessonId'] as String,
      subject: json['subject'] as String,
      text: json['text'] as String,
      createdAt: DateTime.parse(json['createdAt'] as String),
      reminderAt: json['reminderAt'] != null
          ? DateTime.parse(json['reminderAt'] as String)
          : null,
    );
  }
}
