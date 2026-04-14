import 'package:flutter/material.dart';
import '../models/lesson.dart';
import '../theme/app_theme.dart';
import '../utils/time_utils.dart';

class LessonCard extends StatelessWidget {
  final Lesson lesson;
  final VoidCallback onTap;
  final bool isCompact;

  const LessonCard({
    super.key,
    required this.lesson,
    required this.onTap,
    this.isCompact = false,
  });

  @override
  Widget build(BuildContext context) {
    final color = AppTheme.getLessonTypeColor(lesson.lessonTypeIndex);
    final isActive = TimeUtils.isLessonActive(lesson.startTime, lesson.endTime);
    final isPassed = TimeUtils.isLessonPassed(lesson.endTime);

    return GestureDetector(
      onTap: onTap,
      child: Container(
        margin: const EdgeInsets.only(bottom: 12),
        decoration: BoxDecoration(
          color: Theme.of(context).cardTheme.color,
          borderRadius: BorderRadius.circular(16),
          border: isActive
              ? Border.all(color: color, width: 2)
              : Border.all(color: const Color(0xFFE2E8F0)),
          boxShadow: isActive
              ? [
                  BoxShadow(
                    color: color.withValues(alpha: 0.15),
                    blurRadius: 12,
                    offset: const Offset(0, 4),
                  ),
                ]
              : null,
        ),
        child: Opacity(
          opacity: isPassed ? 0.5 : 1.0,
          child: Padding(
            padding: const EdgeInsets.all(16),
            child: Row(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                // Time column
                _buildTimeColumn(context, color, isActive),
                const SizedBox(width: 16),

                // Color indicator
                Container(
                  width: 4,
                  height: isCompact ? 50 : 65,
                  decoration: BoxDecoration(
                    color: color,
                    borderRadius: BorderRadius.circular(2),
                  ),
                ),
                const SizedBox(width: 16),

                // Content
                Expanded(
                  child: _buildContent(context, color, isActive),
                ),

                // Arrow
                Icon(
                  Icons.chevron_right_rounded,
                  color: AppTheme.textLight,
                  size: 20,
                ),
              ],
            ),
          ),
        ),
      ),
    );
  }

  Widget _buildTimeColumn(BuildContext context, Color color, bool isActive) {
    return SizedBox(
      width: 52,
      child: Column(
        children: [
          Text(
            lesson.startTime,
            style: TextStyle(
              fontSize: 15,
              fontWeight: FontWeight.w600,
              color: isActive ? color : AppTheme.textPrimary,
            ),
          ),
          const SizedBox(height: 4),
          Text(
            lesson.endTime,
            style: const TextStyle(
              fontSize: 13,
              color: AppTheme.textLight,
            ),
          ),
          if (isActive) ...[
            const SizedBox(height: 6),
            Container(
              padding: const EdgeInsets.symmetric(horizontal: 6, vertical: 2),
              decoration: BoxDecoration(
                color: color.withValues(alpha: 0.12),
                borderRadius: BorderRadius.circular(4),
              ),
              child: Text(
                'Сейчас',
                style: TextStyle(
                  fontSize: 10,
                  fontWeight: FontWeight.w600,
                  color: color,
                ),
              ),
            ),
          ],
        ],
      ),
    );
  }

  Widget _buildContent(BuildContext context, Color color, bool isActive) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        // Subject name
        Text(
          lesson.subject,
          style: TextStyle(
            fontSize: 16,
            fontWeight: FontWeight.w600,
            color: isActive ? color : AppTheme.textPrimary,
          ),
          maxLines: 2,
          overflow: TextOverflow.ellipsis,
        ),
        const SizedBox(height: 6),

        // Teacher
        Row(
          children: [
            Icon(
              Icons.person_outline_rounded,
              size: 14,
              color: AppTheme.textSecondary,
            ),
            const SizedBox(width: 4),
            Expanded(
              child: Text(
                lesson.teacher,
                style: const TextStyle(
                  fontSize: 13,
                  color: AppTheme.textSecondary,
                ),
                maxLines: 1,
                overflow: TextOverflow.ellipsis,
              ),
            ),
          ],
        ),
        const SizedBox(height: 4),

        // Room + Type
        Row(
          children: [
            // Room
            Icon(
              Icons.location_on_outlined,
              size: 14,
              color: AppTheme.textSecondary,
            ),
            const SizedBox(width: 4),
            Text(
              'Ауд. ${lesson.room}',
              style: const TextStyle(
                fontSize: 13,
                color: AppTheme.textSecondary,
              ),
            ),
            const SizedBox(width: 12),

            // Lesson type badge
            Container(
              padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 2),
              decoration: BoxDecoration(
                color: color.withValues(alpha: 0.1),
                borderRadius: BorderRadius.circular(6),
              ),
              child: Text(
                lesson.lessonType.shortName,
                style: TextStyle(
                  fontSize: 11,
                  fontWeight: FontWeight.w600,
                  color: color,
                ),
              ),
            ),
          ],
        ),
      ],
    );
  }
}
