import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../../providers/app_provider.dart';
import '../../theme/app_theme.dart';
import '../../utils/constants.dart';
import '../../widgets/day_selector.dart';
import '../../widgets/lesson_card.dart';
import '../../widgets/week_indicator.dart';
import '../lesson_detail/lesson_detail_screen.dart';

class ScheduleScreen extends StatelessWidget {
  const ScheduleScreen({super.key});

  @override
  Widget build(BuildContext context) {
    return Consumer<AppProvider>(
      builder: (context, provider, _) {
        return Scaffold(
          appBar: _buildAppBar(context, provider),
          body: Column(
            children: [
              // Week indicator
              Padding(
                padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
                child: Row(
                  children: [
                    WeekIndicator(
                      weekType: provider.currentWeekType,
                      weekNumber: provider.currentWeekNumber,
                    ),
                    const Spacer(),
                    if (provider.isAdmin)
                      _buildAdminBadge(context),
                  ],
                ),
              ),

              // Day selector
              DaySelector(
                selectedDay: provider.selectedDayOfWeek,
                onDaySelected: provider.selectDay,
                todayWeekday: DateTime.now().weekday,
              ),
              const SizedBox(height: 8),

              // Day name header
              Padding(
                padding: const EdgeInsets.symmetric(horizontal: 20, vertical: 8),
                child: Row(
                  children: [
                    Text(
                      AppConstants.getDayName(provider.selectedDayOfWeek),
                      style: Theme.of(context).textTheme.titleLarge?.copyWith(
                        fontWeight: FontWeight.w700,
                      ),
                    ),
                    if (provider.selectedDayOfWeek == DateTime.now().weekday) ...[
                      const SizedBox(width: 8),
                      Container(
                        padding: const EdgeInsets.symmetric(
                          horizontal: 8,
                          vertical: 2,
                        ),
                        decoration: BoxDecoration(
                          color: AppTheme.success.withValues(alpha: 0.1),
                          borderRadius: BorderRadius.circular(6),
                        ),
                        child: const Text(
                          'Сегодня',
                          style: TextStyle(
                            fontSize: 12,
                            fontWeight: FontWeight.w600,
                            color: AppTheme.success,
                          ),
                        ),
                      ),
                    ],
                    const Spacer(),
                    Text(
                      '${provider.todayLessons.length} ${_pluralPairs(provider.todayLessons.length)}',
                      style: const TextStyle(
                        color: AppTheme.textSecondary,
                        fontSize: 14,
                      ),
                    ),
                  ],
                ),
              ),

              // Lessons list
              Expanded(
                child: provider.todayLessons.isEmpty
                    ? _buildEmptyState(context, provider)
                    : _buildLessonsList(context, provider),
              ),
            ],
          ),
          // Bottom navigation
          bottomNavigationBar: _buildBottomNav(context, provider),
        );
      },
    );
  }

  PreferredSizeWidget _buildAppBar(BuildContext context, AppProvider provider) {
    return AppBar(
      title: Column(
        children: [
          const Text('UniSchedule'),
          if (provider.group != null)
            Text(
              '${provider.group} | ${provider.course} курс',
              style: const TextStyle(
                fontSize: 12,
                color: AppTheme.textSecondary,
                fontWeight: FontWeight.w400,
              ),
            ),
        ],
      ),
      actions: [
        IconButton(
          icon: const Icon(Icons.settings_outlined),
          onPressed: () => _showSettings(context, provider),
        ),
      ],
    );
  }

  Widget _buildAdminBadge(BuildContext context) {
    return GestureDetector(
      onTap: () => Navigator.of(context).pushNamed('/admin'),
      child: Container(
        padding: const EdgeInsets.symmetric(horizontal: 10, vertical: 4),
        decoration: BoxDecoration(
          color: AppTheme.warning.withValues(alpha: 0.1),
          borderRadius: BorderRadius.circular(8),
          border: Border.all(
            color: AppTheme.warning.withValues(alpha: 0.3),
          ),
        ),
        child: const Row(
          mainAxisSize: MainAxisSize.min,
          children: [
            Icon(Icons.admin_panel_settings, size: 14, color: AppTheme.warning),
            SizedBox(width: 4),
            Text(
              'Админ',
              style: TextStyle(
                fontSize: 12,
                fontWeight: FontWeight.w600,
                color: AppTheme.warning,
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildEmptyState(BuildContext context, AppProvider provider) {
    final isWeekend = provider.selectedDayOfWeek == 7;
    return Center(
      child: Padding(
        padding: const EdgeInsets.all(40),
        child: Column(
          mainAxisAlignment: MainAxisAlignment.center,
          children: [
            Icon(
              isWeekend ? Icons.weekend_rounded : Icons.event_available_rounded,
              size: 64,
              color: AppTheme.textLight,
            ),
            const SizedBox(height: 16),
            Text(
              isWeekend ? 'Воскресенье — выходной!' : 'Нет пар',
              style: Theme.of(context).textTheme.titleMedium?.copyWith(
                color: AppTheme.textSecondary,
                fontWeight: FontWeight.w600,
              ),
            ),
            const SizedBox(height: 8),
            Text(
              isWeekend
                  ? 'Отдыхай и набирайся сил'
                  : 'В этот день пар нет. Свободный день!',
              style: const TextStyle(
                color: AppTheme.textLight,
                fontSize: 14,
              ),
              textAlign: TextAlign.center,
            ),
          ],
        ),
      ),
    );
  }

  Widget _buildLessonsList(BuildContext context, AppProvider provider) {
    final lessons = provider.todayLessons;
    return ListView.builder(
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
      itemCount: lessons.length,
      itemBuilder: (context, index) {
        final lesson = lessons[index];
        return LessonCard(
          lesson: lesson,
          onTap: () {
            Navigator.of(context).push(
              MaterialPageRoute(
                builder: (_) => LessonDetailScreen(lesson: lesson),
              ),
            );
          },
        );
      },
    );
  }

  Widget _buildBottomNav(BuildContext context, AppProvider provider) {
    return BottomNavigationBar(
      currentIndex: 0,
      items: const [
        BottomNavigationBarItem(
          icon: Icon(Icons.calendar_today_rounded),
          label: 'Расписание',
        ),
        BottomNavigationBarItem(
          icon: Icon(Icons.note_alt_outlined),
          label: 'Заметки',
        ),
        BottomNavigationBarItem(
          icon: Icon(Icons.person_outline_rounded),
          label: 'Профиль',
        ),
      ],
      onTap: (index) {
        if (index == 2) {
          _showSettings(context, provider);
        }
      },
    );
  }

  void _showSettings(BuildContext context, AppProvider provider) {
    showModalBottomSheet(
      context: context,
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(20)),
      ),
      builder: (context) {
        return SafeArea(
          child: Padding(
            padding: const EdgeInsets.all(24),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              crossAxisAlignment: CrossAxisAlignment.start,
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
                  'Настройки',
                  style: Theme.of(context).textTheme.titleLarge?.copyWith(
                    fontWeight: FontWeight.w700,
                  ),
                ),
                const SizedBox(height: 20),

                ListTile(
                  leading: const Icon(Icons.school_outlined),
                  title: const Text('Группа'),
                  subtitle: Text(provider.group ?? '—'),
                  trailing: const Icon(Icons.chevron_right),
                ),
                ListTile(
                  leading: const Icon(Icons.numbers),
                  title: const Text('Курс'),
                  subtitle: Text('${provider.course ?? "—"} курс'),
                  trailing: const Icon(Icons.chevron_right),
                ),
                const Divider(),
                if (!provider.isAdmin)
                  ListTile(
                    leading: const Icon(Icons.admin_panel_settings_outlined),
                    title: const Text('Войти как админ'),
                    onTap: () {
                      Navigator.pop(context);
                      Navigator.of(context).pushNamed('/admin-login');
                    },
                  )
                else
                  ListTile(
                    leading: const Icon(Icons.dashboard_outlined),
                    title: const Text('Админ-панель'),
                    onTap: () {
                      Navigator.pop(context);
                      Navigator.of(context).pushNamed('/admin');
                    },
                  ),
                ListTile(
                  leading: const Icon(Icons.refresh_outlined, color: AppTheme.error),
                  title: const Text('Сбросить все данные',
                    style: TextStyle(color: AppTheme.error),
                  ),
                  onTap: () async {
                    Navigator.pop(context);
                    final confirm = await showDialog<bool>(
                      context: context,
                      builder: (ctx) => AlertDialog(
                        title: const Text('Сбросить данные?'),
                        content: const Text(
                          'Все данные будут удалены: расписание, заметки, настройки.',
                        ),
                        actions: [
                          TextButton(
                            onPressed: () => Navigator.pop(ctx, false),
                            child: const Text('Отмена'),
                          ),
                          TextButton(
                            onPressed: () => Navigator.pop(ctx, true),
                            child: const Text('Сбросить',
                              style: TextStyle(color: AppTheme.error),
                            ),
                          ),
                        ],
                      ),
                    );
                    if (confirm == true && context.mounted) {
                      await provider.resetAll();
                      Navigator.of(context).pushReplacementNamed('/onboarding');
                    }
                  },
                ),
              ],
            ),
          ),
        );
      },
    );
  }

  static String _pluralPairs(int n) {
    if (n % 10 == 1 && n % 100 != 11) return 'пара';
    if (n % 10 >= 2 && n % 10 <= 4 && (n % 100 < 10 || n % 100 >= 20)) {
      return 'пары';
    }
    return 'пар';
  }
}
