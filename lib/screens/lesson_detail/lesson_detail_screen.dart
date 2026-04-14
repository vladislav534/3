import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../../models/lesson.dart';
import '../../models/note.dart';
import '../../providers/app_provider.dart';
import '../../services/notification_service.dart';
import '../../services/schedule_service.dart';
import '../../theme/app_theme.dart';
import '../../utils/constants.dart';
import '../../utils/time_utils.dart';

class LessonDetailScreen extends StatefulWidget {
  final Lesson lesson;

  const LessonDetailScreen({super.key, required this.lesson});

  @override
  State<LessonDetailScreen> createState() => _LessonDetailScreenState();
}

class _LessonDetailScreenState extends State<LessonDetailScreen> {
  final _noteController = TextEditingController();
  bool _reminderEnabled = false;

  @override
  void dispose() {
    _noteController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final color = AppTheme.getLessonTypeColor(widget.lesson.lessonTypeIndex);

    return Consumer<AppProvider>(
      builder: (context, provider, _) {
        final nextLesson = provider.getNextLessonForSubject(
          widget.lesson.subject,
          widget.lesson.dayOfWeek,
          widget.lesson.pairNumber,
        );
        final notes = provider.getNotesForSubject(widget.lesson.subject);

        return Scaffold(
          appBar: AppBar(
            title: const Text('Подробности'),
            backgroundColor: color.withValues(alpha: 0.05),
          ),
          body: SingleChildScrollView(
            padding: const EdgeInsets.all(20),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                // Header card
                _buildHeaderCard(color),
                const SizedBox(height: 20),

                // Details
                _buildDetailsSection(color),
                const SizedBox(height: 20),

                // Next lesson info
                if (nextLesson != null) ...[
                  _buildNextLessonCard(nextLesson),
                  const SizedBox(height: 20),
                ],

                // Reminder section
                _buildReminderSection(color),
                const SizedBox(height: 20),

                // Notes section
                _buildNotesSection(provider, notes),
                const SizedBox(height: 20),

                // Add note
                _buildAddNoteSection(provider),
                const SizedBox(height: 40),
              ],
            ),
          ),
        );
      },
    );
  }

  Widget _buildHeaderCard(Color color) {
    return Container(
      padding: const EdgeInsets.all(24),
      decoration: BoxDecoration(
        gradient: LinearGradient(
          colors: [color, color.withValues(alpha: 0.7)],
          begin: Alignment.topLeft,
          end: Alignment.bottomRight,
        ),
        borderRadius: BorderRadius.circular(20),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          // Lesson type badge
          Container(
            padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 4),
            decoration: BoxDecoration(
              color: Colors.white.withValues(alpha: 0.2),
              borderRadius: BorderRadius.circular(8),
            ),
            child: Text(
              widget.lesson.lessonType.displayName,
              style: const TextStyle(
                color: Colors.white,
                fontSize: 12,
                fontWeight: FontWeight.w600,
              ),
            ),
          ),
          const SizedBox(height: 12),

          // Subject name
          Text(
            widget.lesson.subject,
            style: const TextStyle(
              color: Colors.white,
              fontSize: 24,
              fontWeight: FontWeight.w700,
            ),
          ),
          const SizedBox(height: 16),

          // Time
          Row(
            children: [
              const Icon(Icons.schedule, color: Colors.white70, size: 18),
              const SizedBox(width: 8),
              Text(
                ScheduleService.getPairTime(widget.lesson.pairNumber),
                style: const TextStyle(
                  color: Colors.white,
                  fontSize: 16,
                  fontWeight: FontWeight.w500,
                ),
              ),
              const SizedBox(width: 8),
              Text(
                '(${widget.lesson.pairNumber}-я пара)',
                style: TextStyle(
                  color: Colors.white.withValues(alpha: 0.7),
                  fontSize: 14,
                ),
              ),
            ],
          ),
        ],
      ),
    );
  }

  Widget _buildDetailsSection(Color color) {
    return Container(
      padding: const EdgeInsets.all(20),
      decoration: BoxDecoration(
        color: Theme.of(context).cardTheme.color,
        borderRadius: BorderRadius.circular(16),
        border: Border.all(color: const Color(0xFFE2E8F0)),
      ),
      child: Column(
        children: [
          _detailRow(
            Icons.person_outline_rounded,
            'Преподаватель',
            widget.lesson.teacher,
            color,
          ),
          const Divider(height: 24),
          _detailRow(
            Icons.location_on_outlined,
            'Аудитория',
            widget.lesson.room,
            color,
          ),
          const Divider(height: 24),
          _detailRow(
            Icons.calendar_today_rounded,
            'День',
            AppConstants.getDayName(widget.lesson.dayOfWeek),
            color,
          ),
          const Divider(height: 24),
          _detailRow(
            Icons.repeat_rounded,
            'Неделя',
            widget.lesson.weekType.displayName,
            color,
          ),
          if (widget.lesson.subgroup != null) ...[
            const Divider(height: 24),
            _detailRow(
              Icons.group_outlined,
              'Подгруппа',
              widget.lesson.subgroup!,
              color,
            ),
          ],
        ],
      ),
    );
  }

  Widget _detailRow(IconData icon, String label, String value, Color color) {
    return Row(
      children: [
        Container(
          width: 36,
          height: 36,
          decoration: BoxDecoration(
            color: color.withValues(alpha: 0.1),
            borderRadius: BorderRadius.circular(10),
          ),
          child: Icon(icon, size: 18, color: color),
        ),
        const SizedBox(width: 12),
        Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              label,
              style: const TextStyle(
                fontSize: 12,
                color: AppTheme.textLight,
              ),
            ),
            Text(
              value,
              style: const TextStyle(
                fontSize: 15,
                fontWeight: FontWeight.w500,
              ),
            ),
          ],
        ),
      ],
    );
  }

  Widget _buildNextLessonCard(Lesson nextLesson) {
    final nextDate = TimeUtils.getNextDateForWeekday(nextLesson.dayOfWeek);
    final nextStart = TimeUtils.parseTimeToday(nextLesson.startTime);
    final nextDateTime = DateTime(
      nextDate.year, nextDate.month, nextDate.day,
      nextStart.hour, nextStart.minute,
    );
    final timeUntil = nextDateTime.difference(DateTime.now());

    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: AppTheme.primary.withValues(alpha: 0.05),
        borderRadius: BorderRadius.circular(16),
        border: Border.all(color: AppTheme.primary.withValues(alpha: 0.15)),
      ),
      child: Row(
        children: [
          Container(
            width: 44,
            height: 44,
            decoration: BoxDecoration(
              color: AppTheme.primary.withValues(alpha: 0.1),
              borderRadius: BorderRadius.circular(12),
            ),
            child: const Icon(
              Icons.next_plan_outlined,
              color: AppTheme.primary,
              size: 22,
            ),
          ),
          const SizedBox(width: 14),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  'Следующая пара по этому предмету',
                  style: TextStyle(
                    fontSize: 12,
                    color: AppTheme.textSecondary,
                  ),
                ),
                const SizedBox(height: 2),
                Text(
                  '${AppConstants.getDayName(nextLesson.dayOfWeek)}, ${nextLesson.startTime}',
                  style: const TextStyle(
                    fontSize: 15,
                    fontWeight: FontWeight.w600,
                  ),
                ),
                const SizedBox(height: 2),
                Text(
                  'Через ${TimeUtils.formatDuration(timeUntil)}',
                  style: const TextStyle(
                    fontSize: 13,
                    color: AppTheme.primary,
                    fontWeight: FontWeight.w500,
                  ),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildReminderSection(Color color) {
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: Theme.of(context).cardTheme.color,
        borderRadius: BorderRadius.circular(16),
        border: Border.all(color: const Color(0xFFE2E8F0)),
      ),
      child: Row(
        children: [
          Icon(Icons.notifications_outlined, color: color),
          const SizedBox(width: 12),
          const Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  'Напоминание',
                  style: TextStyle(
                    fontWeight: FontWeight.w600,
                    fontSize: 15,
                  ),
                ),
                Text(
                  'За 15 минут до начала',
                  style: TextStyle(
                    fontSize: 13,
                    color: AppTheme.textSecondary,
                  ),
                ),
              ],
            ),
          ),
          Switch(
            value: _reminderEnabled,
            onChanged: (value) async {
              setState(() => _reminderEnabled = value);
              if (value) {
                final nextDate = TimeUtils.getNextDateForWeekday(
                  widget.lesson.dayOfWeek,
                );
                final start = TimeUtils.parseTimeToday(widget.lesson.startTime);
                final lessonDateTime = DateTime(
                  nextDate.year, nextDate.month, nextDate.day,
                  start.hour, start.minute,
                );
                await NotificationService().scheduleLessonReminder(
                  lesson: widget.lesson,
                  lessonDateTime: lessonDateTime,
                );
              } else {
                await NotificationService().cancelNotification(widget.lesson.id);
              }
            },
            activeColor: color,
          ),
        ],
      ),
    );
  }

  Widget _buildNotesSection(AppProvider provider, List<Note> notes) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          'Заметки (${notes.length})',
          style: Theme.of(context).textTheme.titleMedium?.copyWith(
            fontWeight: FontWeight.w600,
          ),
        ),
        const SizedBox(height: 12),
        if (notes.isEmpty)
          Container(
            width: double.infinity,
            padding: const EdgeInsets.all(20),
            decoration: BoxDecoration(
              color: AppTheme.background,
              borderRadius: BorderRadius.circular(12),
            ),
            child: const Text(
              'Пока нет заметок. Добавь заметку к следующей паре!',
              style: TextStyle(
                color: AppTheme.textLight,
                fontSize: 14,
              ),
              textAlign: TextAlign.center,
            ),
          )
        else
          ...notes.map((note) => _buildNoteCard(provider, note)),
      ],
    );
  }

  Widget _buildNoteCard(AppProvider provider, Note note) {
    return Container(
      margin: const EdgeInsets.only(bottom: 8),
      padding: const EdgeInsets.all(14),
      decoration: BoxDecoration(
        color: AppTheme.background,
        borderRadius: BorderRadius.circular(12),
      ),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const Icon(Icons.note_outlined, size: 18, color: AppTheme.textSecondary),
          const SizedBox(width: 10),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  note.text,
                  style: const TextStyle(fontSize: 14),
                ),
                const SizedBox(height: 4),
                Text(
                  _formatDate(note.createdAt),
                  style: const TextStyle(
                    fontSize: 12,
                    color: AppTheme.textLight,
                  ),
                ),
              ],
            ),
          ),
          IconButton(
            icon: const Icon(Icons.delete_outline, size: 18),
            color: AppTheme.error,
            onPressed: () => provider.deleteNote(note.id),
          ),
        ],
      ),
    );
  }

  Widget _buildAddNoteSection(AppProvider provider) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          'Добавить заметку на следующую пару',
          style: Theme.of(context).textTheme.titleMedium?.copyWith(
            fontWeight: FontWeight.w600,
          ),
        ),
        const SizedBox(height: 12),
        TextField(
          controller: _noteController,
          maxLines: 3,
          decoration: const InputDecoration(
            hintText: 'Что нужно не забыть...',
            alignLabelWithHint: true,
          ),
        ),
        const SizedBox(height: 12),
        SizedBox(
          width: double.infinity,
          child: ElevatedButton.icon(
            onPressed: () async {
              if (_noteController.text.trim().isEmpty) return;

              await provider.addNote(
                lessonId: widget.lesson.id,
                subject: widget.lesson.subject,
                text: _noteController.text.trim(),
              );
              _noteController.clear();

              if (mounted) {
                ScaffoldMessenger.of(context).showSnackBar(
                  const SnackBar(
                    content: Text('Заметка добавлена!'),
                    behavior: SnackBarBehavior.floating,
                  ),
                );
              }
            },
            icon: const Icon(Icons.add_rounded, size: 20),
            label: const Text('Добавить заметку'),
          ),
        ),
      ],
    );
  }

  String _formatDate(DateTime date) {
    final months = [
      'янв', 'фев', 'мар', 'апр', 'май', 'июн',
      'июл', 'авг', 'сен', 'окт', 'ноя', 'дек',
    ];
    return '${date.day} ${months[date.month - 1]}, ${date.hour}:${date.minute.toString().padLeft(2, '0')}';
  }
}
