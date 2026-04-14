import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../../providers/app_provider.dart';
import '../../theme/app_theme.dart';
import '../../utils/constants.dart';
import 'edit_schedule_screen.dart';
import 'send_notification_screen.dart';

class AdminPanelScreen extends StatelessWidget {
  const AdminPanelScreen({super.key});

  @override
  Widget build(BuildContext context) {
    return Consumer<AppProvider>(
      builder: (context, provider, _) {
        final lessonCount = provider.schedule?.lessons.length ?? 0;

        return Scaffold(
          appBar: AppBar(
            title: const Text('Админ-панель'),
            actions: [
              TextButton.icon(
                onPressed: () async {
                  await provider.logoutAdmin();
                  if (context.mounted) {
                    Navigator.of(context).pushReplacementNamed(
                      provider.onboardingComplete ? '/schedule' : '/onboarding',
                    );
                  }
                },
                icon: const Icon(Icons.logout, size: 18),
                label: const Text('Выйти'),
              ),
            ],
          ),
          body: SingleChildScrollView(
            padding: const EdgeInsets.all(20),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                // Stats header
                _buildStatsRow(provider, lessonCount),
                const SizedBox(height: 24),

                // Actions
                Text(
                  'Управление',
                  style: Theme.of(context).textTheme.titleLarge?.copyWith(
                    fontWeight: FontWeight.w700,
                  ),
                ),
                const SizedBox(height: 16),

                _buildActionCard(
                  context,
                  icon: Icons.edit_calendar_rounded,
                  color: AppTheme.primary,
                  title: 'Редактировать расписание',
                  subtitle: 'Добавить, изменить или удалить пары',
                  onTap: () {
                    Navigator.of(context).push(
                      MaterialPageRoute(
                        builder: (_) => const EditScheduleScreen(),
                      ),
                    );
                  },
                ),
                const SizedBox(height: 12),

                _buildActionCard(
                  context,
                  icon: Icons.notifications_active_rounded,
                  color: AppTheme.secondary,
                  title: 'Отправить уведомление',
                  subtitle: 'Группе или всем студентам',
                  onTap: () {
                    Navigator.of(context).push(
                      MaterialPageRoute(
                        builder: (_) => const SendNotificationScreen(),
                      ),
                    );
                  },
                ),
                const SizedBox(height: 12),

                _buildActionCard(
                  context,
                  icon: Icons.upload_file_rounded,
                  color: AppTheme.labColor,
                  title: 'Загрузить расписание с фото',
                  subtitle: 'Скоро — распознавание расписания с картинки',
                  onTap: () {
                    ScaffoldMessenger.of(context).showSnackBar(
                      const SnackBar(
                        content: Text('Функция в разработке'),
                        behavior: SnackBarBehavior.floating,
                      ),
                    );
                  },
                ),
                const SizedBox(height: 12),

                _buildActionCard(
                  context,
                  icon: Icons.delete_sweep_rounded,
                  color: AppTheme.error,
                  title: 'Очистить расписание',
                  subtitle: 'Удалить все пары для группы',
                  onTap: () => _confirmClearSchedule(context, provider),
                ),
                const SizedBox(height: 24),

                // Current schedule overview
                Text(
                  'Текущее расписание',
                  style: Theme.of(context).textTheme.titleLarge?.copyWith(
                    fontWeight: FontWeight.w700,
                  ),
                ),
                const SizedBox(height: 16),

                _buildScheduleOverview(context, provider),
              ],
            ),
          ),
        );
      },
    );
  }

  Widget _buildStatsRow(AppProvider provider, int lessonCount) {
    return Row(
      children: [
        Expanded(
          child: _statCard(
            'Группа',
            provider.group ?? '—',
            Icons.group_rounded,
            AppTheme.primary,
          ),
        ),
        const SizedBox(width: 12),
        Expanded(
          child: _statCard(
            'Пары',
            '$lessonCount',
            Icons.class_rounded,
            AppTheme.secondary,
          ),
        ),
        const SizedBox(width: 12),
        Expanded(
          child: _statCard(
            'Курс',
            '${provider.course ?? "—"}',
            Icons.school_rounded,
            AppTheme.labColor,
          ),
        ),
      ],
    );
  }

  Widget _statCard(String label, String value, IconData icon, Color color) {
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: color.withValues(alpha: 0.08),
        borderRadius: BorderRadius.circular(14),
      ),
      child: Column(
        children: [
          Icon(icon, color: color, size: 24),
          const SizedBox(height: 8),
          Text(
            value,
            style: TextStyle(
              fontSize: 18,
              fontWeight: FontWeight.w700,
              color: color,
            ),
          ),
          Text(
            label,
            style: const TextStyle(
              fontSize: 12,
              color: AppTheme.textSecondary,
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildActionCard(
    BuildContext context, {
    required IconData icon,
    required Color color,
    required String title,
    required String subtitle,
    required VoidCallback onTap,
  }) {
    return GestureDetector(
      onTap: onTap,
      child: Container(
        padding: const EdgeInsets.all(16),
        decoration: BoxDecoration(
          color: Theme.of(context).cardTheme.color,
          borderRadius: BorderRadius.circular(14),
          border: Border.all(color: const Color(0xFFE2E8F0)),
        ),
        child: Row(
          children: [
            Container(
              width: 48,
              height: 48,
              decoration: BoxDecoration(
                color: color.withValues(alpha: 0.1),
                borderRadius: BorderRadius.circular(12),
              ),
              child: Icon(icon, color: color),
            ),
            const SizedBox(width: 14),
            Expanded(
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Text(
                    title,
                    style: const TextStyle(
                      fontWeight: FontWeight.w600,
                      fontSize: 15,
                    ),
                  ),
                  const SizedBox(height: 2),
                  Text(
                    subtitle,
                    style: const TextStyle(
                      fontSize: 13,
                      color: AppTheme.textSecondary,
                    ),
                  ),
                ],
              ),
            ),
            const Icon(Icons.chevron_right, color: AppTheme.textLight),
          ],
        ),
      ),
    );
  }

  Widget _buildScheduleOverview(BuildContext context, AppProvider provider) {
    if (provider.schedule == null) {
      return const Center(
        child: Text('Расписание не загружено'),
      );
    }

    return Column(
      children: List.generate(6, (index) {
        final day = index + 1;
        final lessons = provider.schedule!.getLessonsForDay(
          day,
          provider.currentWeekType,
        );
        return Container(
          margin: const EdgeInsets.only(bottom: 8),
          padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 12),
          decoration: BoxDecoration(
            color: AppTheme.background,
            borderRadius: BorderRadius.circular(10),
          ),
          child: Row(
            children: [
              SizedBox(
                width: 100,
                child: Text(
                  AppConstants.getDayName(day),
                  style: const TextStyle(
                    fontWeight: FontWeight.w500,
                    fontSize: 14,
                  ),
                ),
              ),
              Text(
                '${lessons.length} ${_plural(lessons.length)}',
                style: const TextStyle(
                  color: AppTheme.textSecondary,
                  fontSize: 14,
                ),
              ),
              const Spacer(),
              if (lessons.isNotEmpty)
                Text(
                  '${lessons.first.startTime} — ${lessons.last.endTime}',
                  style: const TextStyle(
                    fontSize: 13,
                    color: AppTheme.textLight,
                  ),
                ),
            ],
          ),
        );
      }),
    );
  }

  void _confirmClearSchedule(BuildContext context, AppProvider provider) {
    showDialog(
      context: context,
      builder: (ctx) => AlertDialog(
        title: const Text('Очистить расписание?'),
        content: const Text(
          'Все пары будут удалены. Это действие нельзя отменить.',
        ),
        actions: [
          TextButton(
            onPressed: () => Navigator.pop(ctx),
            child: const Text('Отмена'),
          ),
          TextButton(
            onPressed: () async {
              await provider.importSchedule([]);
              if (ctx.mounted) Navigator.pop(ctx);
            },
            child: const Text(
              'Удалить',
              style: TextStyle(color: AppTheme.error),
            ),
          ),
        ],
      ),
    );
  }

  static String _plural(int n) {
    if (n % 10 == 1 && n % 100 != 11) return 'пара';
    if (n % 10 >= 2 && n % 10 <= 4 && (n % 100 < 10 || n % 100 >= 20)) {
      return 'пары';
    }
    return 'пар';
  }
}
