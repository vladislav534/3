import 'package:flutter/material.dart';
import '../theme/app_theme.dart';
import '../utils/constants.dart';

class DaySelector extends StatelessWidget {
  final int selectedDay;
  final ValueChanged<int> onDaySelected;
  final int todayWeekday;

  const DaySelector({
    super.key,
    required this.selectedDay,
    required this.onDaySelected,
    required this.todayWeekday,
  });

  @override
  Widget build(BuildContext context) {
    return SizedBox(
      height: 72,
      child: ListView.builder(
        scrollDirection: Axis.horizontal,
        padding: const EdgeInsets.symmetric(horizontal: 16),
        itemCount: 7,
        itemBuilder: (context, index) {
          final day = index + 1;
          final isSelected = day == selectedDay;
          final isToday = day == todayWeekday;

          return Padding(
            padding: const EdgeInsets.only(right: 8),
            child: GestureDetector(
              onTap: () => onDaySelected(day),
              child: AnimatedContainer(
                duration: const Duration(milliseconds: 200),
                width: 48,
                decoration: BoxDecoration(
                  color: isSelected
                      ? AppTheme.primary
                      : isToday
                          ? AppTheme.primary.withValues(alpha: 0.08)
                          : Colors.transparent,
                  borderRadius: BorderRadius.circular(14),
                  border: isToday && !isSelected
                      ? Border.all(color: AppTheme.primary.withValues(alpha: 0.3))
                      : null,
                ),
                child: Column(
                  mainAxisAlignment: MainAxisAlignment.center,
                  children: [
                    Text(
                      AppConstants.getDayNameShort(day),
                      style: TextStyle(
                        fontSize: 12,
                        fontWeight: FontWeight.w500,
                        color: isSelected
                            ? Colors.white.withValues(alpha: 0.8)
                            : AppTheme.textSecondary,
                      ),
                    ),
                    const SizedBox(height: 4),
                    Text(
                      _getDayNumber(day),
                      style: TextStyle(
                        fontSize: 18,
                        fontWeight: FontWeight.w700,
                        color: isSelected
                            ? Colors.white
                            : isToday
                                ? AppTheme.primary
                                : AppTheme.textPrimary,
                      ),
                    ),
                    if (isToday) ...[
                      const SizedBox(height: 2),
                      Container(
                        width: 4,
                        height: 4,
                        decoration: BoxDecoration(
                          color: isSelected ? Colors.white : AppTheme.primary,
                          shape: BoxShape.circle,
                        ),
                      ),
                    ],
                  ],
                ),
              ),
            ),
          );
        },
      ),
    );
  }

  String _getDayNumber(int weekday) {
    final now = DateTime.now();
    final currentWeekday = now.weekday;
    final diff = weekday - currentWeekday;
    final date = now.add(Duration(days: diff));
    return date.day.toString();
  }
}
