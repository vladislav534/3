import 'package:flutter/material.dart';
import '../../theme/app_theme.dart';

class SendNotificationScreen extends StatefulWidget {
  const SendNotificationScreen({super.key});

  @override
  State<SendNotificationScreen> createState() =>
      _SendNotificationScreenState();
}

class _SendNotificationScreenState extends State<SendNotificationScreen> {
  final _titleController = TextEditingController();
  final _bodyController = TextEditingController();
  String _targetType = 'all'; // 'all' or 'group'
  final _groupController = TextEditingController();
  bool _isSending = false;

  @override
  void dispose() {
    _titleController.dispose();
    _bodyController.dispose();
    _groupController.dispose();
    super.dispose();
  }

  Future<void> _send() async {
    if (_titleController.text.isEmpty || _bodyController.text.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('Заполните заголовок и текст'),
          behavior: SnackBarBehavior.floating,
        ),
      );
      return;
    }

    if (_targetType == 'group' && _groupController.text.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('Укажите группу'),
          behavior: SnackBarBehavior.floating,
        ),
      );
      return;
    }

    setState(() => _isSending = true);

    // In production: send push via Firebase Cloud Messaging
    await Future.delayed(const Duration(seconds: 1));

    if (!mounted) return;

    setState(() => _isSending = false);

    showDialog(
      context: context,
      builder: (ctx) => AlertDialog(
        icon: const Icon(Icons.check_circle, color: AppTheme.success, size: 48),
        title: const Text('Уведомление отправлено!'),
        content: Text(
          _targetType == 'all'
              ? 'Уведомление отправлено всем студентам'
              : 'Уведомление отправлено группе ${_groupController.text}',
        ),
        actions: [
          TextButton(
            onPressed: () {
              Navigator.pop(ctx);
              Navigator.pop(context);
            },
            child: const Text('Готово'),
          ),
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Уведомление'),
      ),
      body: SingleChildScrollView(
        padding: const EdgeInsets.all(24),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            // Target selection
            Text(
              'Кому отправить',
              style: Theme.of(context).textTheme.titleMedium?.copyWith(
                fontWeight: FontWeight.w600,
              ),
            ),
            const SizedBox(height: 12),

            Row(
              children: [
                Expanded(
                  child: _targetOption(
                    icon: Icons.people_rounded,
                    label: 'Всем',
                    value: 'all',
                  ),
                ),
                const SizedBox(width: 12),
                Expanded(
                  child: _targetOption(
                    icon: Icons.group_rounded,
                    label: 'Группе',
                    value: 'group',
                  ),
                ),
              ],
            ),

            if (_targetType == 'group') ...[
              const SizedBox(height: 16),
              TextField(
                controller: _groupController,
                decoration: const InputDecoration(
                  labelText: 'Номер группы',
                  hintText: 'ЭН-251',
                  prefixIcon: Icon(Icons.group_outlined),
                ),
              ),
            ],

            const SizedBox(height: 24),

            // Title
            TextField(
              controller: _titleController,
              decoration: const InputDecoration(
                labelText: 'Заголовок',
                hintText: 'Изменение расписания',
              ),
            ),
            const SizedBox(height: 16),

            // Body
            TextField(
              controller: _bodyController,
              maxLines: 5,
              decoration: const InputDecoration(
                labelText: 'Текст уведомления',
                hintText: 'Завтра 3-я пара по математике отменена...',
                alignLabelWithHint: true,
              ),
            ),
            const SizedBox(height: 32),

            // Send button
            SizedBox(
              height: 56,
              child: ElevatedButton.icon(
                onPressed: _isSending ? null : _send,
                icon: _isSending
                    ? const SizedBox(
                        width: 20,
                        height: 20,
                        child: CircularProgressIndicator(
                          color: Colors.white,
                          strokeWidth: 2,
                        ),
                      )
                    : const Icon(Icons.send_rounded),
                label: Text(_isSending ? 'Отправка...' : 'Отправить'),
              ),
            ),
          ],
        ),
      ),
    );
  }

  Widget _targetOption({
    required IconData icon,
    required String label,
    required String value,
  }) {
    final isSelected = _targetType == value;
    return GestureDetector(
      onTap: () => setState(() => _targetType = value),
      child: AnimatedContainer(
        duration: const Duration(milliseconds: 200),
        padding: const EdgeInsets.symmetric(vertical: 16),
        decoration: BoxDecoration(
          color: isSelected
              ? AppTheme.primary.withValues(alpha: 0.08)
              : AppTheme.background,
          borderRadius: BorderRadius.circular(14),
          border: Border.all(
            color: isSelected ? AppTheme.primary : const Color(0xFFE2E8F0),
            width: isSelected ? 2 : 1,
          ),
        ),
        child: Column(
          children: [
            Icon(
              icon,
              color: isSelected ? AppTheme.primary : AppTheme.textSecondary,
              size: 28,
            ),
            const SizedBox(height: 8),
            Text(
              label,
              style: TextStyle(
                fontWeight: isSelected ? FontWeight.w600 : FontWeight.w400,
                color: isSelected ? AppTheme.primary : AppTheme.textSecondary,
              ),
            ),
          ],
        ),
      ),
    );
  }
}
