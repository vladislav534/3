import 'package:flutter/material.dart';
import '../models/lesson.dart';
import '../theme/app_theme.dart';

class WeekIndicator extends StatelessWidget {
  final WeekType weekType;
  final int weekNumber;

  const WeekIndicator({
    super.key,
    required this.weekType,
    required this.weekNumber,
  });

  @override
  Widget build(BuildContext context) {
    final isUpper = weekType == WeekType.upper;

    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 6),
      decoration: BoxDecoration(
        color: isUpper
            ? AppTheme.primary.withValues(alpha: 0.1)
            : AppTheme.secondary.withValues(alpha: 0.1),
        borderRadius: BorderRadius.circular(8),
      ),
      child: Row(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(
            isUpper ? Icons.arrow_upward_rounded : Icons.arrow_downward_rounded,
            size: 14,
            color: isUpper ? AppTheme.primary : AppTheme.secondary,
          ),
          const SizedBox(width: 4),
          Text(
            '${isUpper ? "Верхняя" : "Нижняя"} неделя',
            style: TextStyle(
              fontSize: 12,
              fontWeight: FontWeight.w600,
              color: isUpper ? AppTheme.primary : AppTheme.secondary,
            ),
          ),
          const SizedBox(width: 6),
          Container(
            padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 1),
            decoration: BoxDecoration(
              color: (isUpper ? AppTheme.primary : AppTheme.secondary)
                  .withValues(alpha: 0.15),
              borderRadius: BorderRadius.circular(4),
            ),
            child: Text(
              '$weekNumber',
              style: TextStyle(
                fontSize: 11,
                fontWeight: FontWeight.w700,
                color: isUpper ? AppTheme.primary : AppTheme.secondary,
              ),
            ),
          ),
        ],
      ),
    );
  }
}
