import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../../models/lesson.dart';
import '../../providers/app_provider.dart';
import '../../services/schedule_service.dart';
import '../../theme/app_theme.dart';
import '../../utils/constants.dart';

class EditScheduleScreen extends StatefulWidget {
  const EditScheduleScreen({super.key});

  @override
  State<EditScheduleScreen> createState() => _EditScheduleScreenState();
}

class _EditScheduleScreenState extends State<EditScheduleScreen>
    with SingleTickerProviderStateMixin {
  late TabController _tabController;

  @override
  void initState() {
    super.initState();
    _tabController = TabController(length: 6, vsync: this);
  }

  @override
  void dispose() {
    _tabController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Consumer<AppProvider>(
      builder: (context, provider, _) {
        return Scaffold(
          appBar: AppBar(
            title: const Text('Редактировать расписание'),
            bottom: TabBar(
              controller: _tabController,
              isScrollable: true,
              tabs: List.generate(6, (i) {
                return Tab(text: AppConstants.getDayNameShort(i + 1));
              }),
            ),
          ),
          body: TabBarView(
            controller: _tabController,
            children: List.generate(6, (index) {
              final day = index + 1;
              return _buildDayTab(context, provider, day);
            }),
          ),
          floatingActionButton: FloatingActionButton.extended(
            onPressed: () => _showAddLessonDialog(context, provider),
            icon: const Icon(Icons.add),
            label: const Text('Добавить пару'),
          ),
        );
      },
    );
  }

  Widget _buildDayTab(BuildContext context, AppProvider provider, int day) {
    final allLessons = provider.schedule?.lessons
            .where((l) => l.dayOfWeek == day)
            .toList() ??
        [];
    allLessons.sort((a, b) => a.pairNumber.compareTo(b.pairNumber));

    if (allLessons.isEmpty) {
      return const Center(
        child: Text(
          'Нет пар в этот день',
          style: TextStyle(color: AppTheme.textSecondary),
        ),
      );
    }

    return ListView.builder(
      padding: const EdgeInsets.all(16),
      itemCount: allLessons.length,
      itemBuilder: (context, index) {
        final lesson = allLessons[index];
        final color = AppTheme.getLessonTypeColor(lesson.lessonTypeIndex);

        return Card(
          margin: const EdgeInsets.only(bottom: 8),
          child: ListTile(
            leading: Container(
              width: 4,
              height: 40,
              decoration: BoxDecoration(
                color: color,
                borderRadius: BorderRadius.circular(2),
              ),
            ),
            title: Text(
              lesson.subject,
              style: const TextStyle(fontWeight: FontWeight.w600),
            ),
            subtitle: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  '${lesson.startTime} — ${lesson.endTime} | ${lesson.teacher}',
                ),
                Text(
                  'Ауд. ${lesson.room} | ${lesson.lessonType.shortName} | ${lesson.weekType.displayName}',
                  style: const TextStyle(fontSize: 12),
                ),
              ],
            ),
            trailing: PopupMenuButton<String>(
              onSelected: (value) {
                if (value == 'delete') {
                  _confirmDelete(context, provider, lesson);
                }
              },
              itemBuilder: (context) => [
                const PopupMenuItem(
                  value: 'delete',
                  child: Row(
                    children: [
                      Icon(Icons.delete, color: AppTheme.error, size: 18),
                      SizedBox(width: 8),
                      Text('Удалить', style: TextStyle(color: AppTheme.error)),
                    ],
                  ),
                ),
              ],
            ),
            isThreeLine: true,
          ),
        );
      },
    );
  }

  void _confirmDelete(
    BuildContext context,
    AppProvider provider,
    Lesson lesson,
  ) {
    showDialog(
      context: context,
      builder: (ctx) => AlertDialog(
        title: const Text('Удалить пару?'),
        content: Text('${lesson.subject}\n${lesson.teacher}'),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(ctx),
            child: const Text('Отмена'),
          ),
          TextButton(
            onPressed: () {
              provider.deleteLesson(lesson.id);
              Navigator.pop(ctx);
            },
            child: const Text('Удалить',
                style: TextStyle(color: AppTheme.error)),
          ),
        ],
      ),
    );
  }

  void _showAddLessonDialog(BuildContext context, AppProvider provider) {
    final subjectController = TextEditingController();
    final teacherController = TextEditingController();
    final roomController = TextEditingController();
    int selectedDay = _tabController.index + 1;
    int selectedPair = 1;
    LessonType selectedType = LessonType.lecture;
    WeekType selectedWeekType = WeekType.both;

    showModalBottomSheet(
      context: context,
      isScrollControlled: true,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(20)),
      ),
      builder: (ctx) {
        return StatefulBuilder(
          builder: (ctx, setModalState) {
            return Padding(
              padding: EdgeInsets.only(
                left: 24,
                right: 24,
                top: 24,
                bottom: MediaQuery.of(ctx).viewInsets.bottom + 24,
              ),
              child: SingleChildScrollView(
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  crossAxisAlignment: CrossAxisAlignment.stretch,
                  children: [
                    Center(
                      child: Container(
                        width: 40,
                        height: 4,
                        decoration: BoxDecoration(
                          color: AppTheme.textLight,
                          borderRadius: BorderRadius.circular(2),
                        ),
                      ),
                    ),
                    const SizedBox(height: 20),
                    Text(
                      'Новая пара',
                      style: Theme.of(ctx).textTheme.titleLarge?.copyWith(
                        fontWeight: FontWeight.w700,
                      ),
                    ),
                    const SizedBox(height: 20),

                    TextField(
                      controller: subjectController,
                      decoration: const InputDecoration(
                        labelText: 'Предмет',
                        hintText: 'Математический анализ',
                      ),
                    ),
                    const SizedBox(height: 12),
                    TextField(
                      controller: teacherController,
                      decoration: const InputDecoration(
                        labelText: 'Преподаватель',
                        hintText: 'Иванов И.И.',
                      ),
                    ),
                    const SizedBox(height: 12),
                    TextField(
                      controller: roomController,
                      decoration: const InputDecoration(
                        labelText: 'Аудитория',
                        hintText: '301',
                      ),
                    ),
                    const SizedBox(height: 16),

                    // Day selector
                    const Text('День недели',
                        style: TextStyle(fontWeight: FontWeight.w600)),
                    const SizedBox(height: 8),
                    Wrap(
                      spacing: 8,
                      children: List.generate(6, (i) {
                        return ChoiceChip(
                          label: Text(AppConstants.getDayNameShort(i + 1)),
                          selected: selectedDay == i + 1,
                          onSelected: (s) {
                            if (s) setModalState(() => selectedDay = i + 1);
                          },
                        );
                      }),
                    ),
                    const SizedBox(height: 16),

                    // Pair number
                    const Text('Номер пары',
                        style: TextStyle(fontWeight: FontWeight.w600)),
                    const SizedBox(height: 8),
                    Wrap(
                      spacing: 8,
                      children: List.generate(7, (i) {
                        return ChoiceChip(
                          label: Text('${i + 1}'),
                          selected: selectedPair == i + 1,
                          onSelected: (s) {
                            if (s) setModalState(() => selectedPair = i + 1);
                          },
                        );
                      }),
                    ),
                    const SizedBox(height: 16),

                    // Lesson type
                    const Text('Тип занятия',
                        style: TextStyle(fontWeight: FontWeight.w600)),
                    const SizedBox(height: 8),
                    Wrap(
                      spacing: 8,
                      children: LessonType.values.map((type) {
                        return ChoiceChip(
                          label: Text(type.displayName),
                          selected: selectedType == type,
                          onSelected: (s) {
                            if (s) setModalState(() => selectedType = type);
                          },
                        );
                      }).toList(),
                    ),
                    const SizedBox(height: 16),

                    // Week type
                    const Text('Неделя',
                        style: TextStyle(fontWeight: FontWeight.w600)),
                    const SizedBox(height: 8),
                    Wrap(
                      spacing: 8,
                      children: WeekType.values.map((week) {
                        return ChoiceChip(
                          label: Text(week.displayName),
                          selected: selectedWeekType == week,
                          onSelected: (s) {
                            if (s) setModalState(() => selectedWeekType = week);
                          },
                        );
                      }).toList(),
                    ),
                    const SizedBox(height: 24),

                    ElevatedButton(
                      onPressed: () {
                        if (subjectController.text.isEmpty ||
                            teacherController.text.isEmpty ||
                            roomController.text.isEmpty) {
                          ScaffoldMessenger.of(ctx).showSnackBar(
                            const SnackBar(
                              content: Text('Заполните все поля'),
                              behavior: SnackBarBehavior.floating,
                            ),
                          );
                          return;
                        }

                        final lesson = ScheduleService.createLesson(
                          subject: subjectController.text.trim(),
                          teacher: teacherController.text.trim(),
                          room: roomController.text.trim(),
                          type: selectedType,
                          dayOfWeek: selectedDay,
                          pairNumber: selectedPair,
                          weekType: selectedWeekType,
                        );

                        provider.addLesson(lesson);
                        Navigator.pop(ctx);
                      },
                      child: const Text('Добавить'),
                    ),
                    const SizedBox(height: 8),
                  ],
                ),
              ),
            );
          },
        );
      },
    );
  }
}
