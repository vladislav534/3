import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../../providers/app_provider.dart';
import '../../services/group_parser.dart';
import '../../theme/app_theme.dart';

class OnboardingScreen extends StatefulWidget {
  const OnboardingScreen({super.key});

  @override
  State<OnboardingScreen> createState() => _OnboardingScreenState();
}

class _OnboardingScreenState extends State<OnboardingScreen> {
  final _groupController = TextEditingController();
  final _formKey = GlobalKey<FormState>();
  int _selectedCourse = 1;
  bool _isLoading = false;
  String? _groupError;

  @override
  void dispose() {
    _groupController.dispose();
    super.dispose();
  }

  Future<void> _continue() async {
    final normalized = GroupParser.normalize(_groupController.text);
    if (normalized == null) {
      setState(() {
        _groupError = 'Введите группу в формате: ЭН-251, эн251, ЭН 251';
      });
      return;
    }

    setState(() {
      _isLoading = true;
      _groupError = null;
    });

    final provider = context.read<AppProvider>();
    await provider.completeOnboarding(
      course: _selectedCourse,
      group: normalized,
    );

    if (mounted) {
      Navigator.of(context).pushReplacementNamed('/schedule');
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: SafeArea(
        child: SingleChildScrollView(
          padding: const EdgeInsets.symmetric(horizontal: 24),
          child: Form(
            key: _formKey,
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.stretch,
              children: [
                const SizedBox(height: 60),

                // Logo / Title
                Container(
                  width: 80,
                  height: 80,
                  decoration: BoxDecoration(
                    gradient: const LinearGradient(
                      colors: [AppTheme.primary, AppTheme.secondary],
                      begin: Alignment.topLeft,
                      end: Alignment.bottomRight,
                    ),
                    borderRadius: BorderRadius.circular(20),
                  ),
                  child: const Icon(
                    Icons.school_rounded,
                    color: Colors.white,
                    size: 40,
                  ),
                ),
                const SizedBox(height: 24),

                Text(
                  'UniSchedule',
                  style: Theme.of(context).textTheme.headlineLarge?.copyWith(
                    fontWeight: FontWeight.bold,
                    color: AppTheme.textPrimary,
                  ),
                  textAlign: TextAlign.center,
                ),
                const SizedBox(height: 8),

                Text(
                  'Твоё расписание всегда под рукой',
                  style: Theme.of(context).textTheme.bodyLarge?.copyWith(
                    color: AppTheme.textSecondary,
                  ),
                  textAlign: TextAlign.center,
                ),
                const SizedBox(height: 48),

                // Course selection
                Text(
                  'Выбери курс',
                  style: Theme.of(context).textTheme.titleMedium?.copyWith(
                    fontWeight: FontWeight.w600,
                  ),
                ),
                const SizedBox(height: 12),

                Wrap(
                  spacing: 10,
                  children: List.generate(6, (index) {
                    final course = index + 1;
                    final isSelected = _selectedCourse == course;
                    return ChoiceChip(
                      label: Text(
                        '$course курс',
                        style: TextStyle(
                          color: isSelected ? AppTheme.primary : AppTheme.textSecondary,
                          fontWeight: isSelected ? FontWeight.w600 : FontWeight.normal,
                        ),
                      ),
                      selected: isSelected,
                      onSelected: (selected) {
                        if (selected) {
                          setState(() => _selectedCourse = course);
                        }
                      },
                      selectedColor: AppTheme.primary.withValues(alpha: 0.12),
                      backgroundColor: AppTheme.background,
                      shape: RoundedRectangleBorder(
                        borderRadius: BorderRadius.circular(10),
                        side: BorderSide(
                          color: isSelected ? AppTheme.primary : const Color(0xFFE2E8F0),
                        ),
                      ),
                    );
                  }),
                ),
                const SizedBox(height: 32),

                // Group input
                Text(
                  'Введи свою группу',
                  style: Theme.of(context).textTheme.titleMedium?.copyWith(
                    fontWeight: FontWeight.w600,
                  ),
                ),
                const SizedBox(height: 12),

                TextField(
                  controller: _groupController,
                  textCapitalization: TextCapitalization.characters,
                  decoration: InputDecoration(
                    hintText: 'Например: ЭН-251, эн251, ЭН 251',
                    prefixIcon: const Icon(Icons.group_rounded),
                    errorText: _groupError,
                    suffixIcon: _groupController.text.isNotEmpty
                        ? IconButton(
                            icon: const Icon(Icons.clear),
                            onPressed: () {
                              _groupController.clear();
                              setState(() => _groupError = null);
                            },
                          )
                        : null,
                  ),
                  onChanged: (value) {
                    setState(() {
                      _groupError = null;
                    });
                  },
                ),

                // Normalized preview
                if (_groupController.text.isNotEmpty &&
                    GroupParser.isValid(_groupController.text))
                  Padding(
                    padding: const EdgeInsets.only(top: 8),
                    child: Row(
                      children: [
                        const Icon(
                          Icons.check_circle,
                          color: AppTheme.success,
                          size: 16,
                        ),
                        const SizedBox(width: 6),
                        Text(
                          'Группа: ${GroupParser.normalize(_groupController.text)}',
                          style: const TextStyle(
                            color: AppTheme.success,
                            fontSize: 13,
                          ),
                        ),
                      ],
                    ),
                  ),

                const SizedBox(height: 48),

                // Continue button
                SizedBox(
                  height: 56,
                  child: ElevatedButton(
                    onPressed: _isLoading ? null : _continue,
                    child: _isLoading
                        ? const SizedBox(
                            width: 24,
                            height: 24,
                            child: CircularProgressIndicator(
                              color: Colors.white,
                              strokeWidth: 2,
                            ),
                          )
                        : const Text('Продолжить'),
                  ),
                ),
                const SizedBox(height: 16),

                // Admin link
                TextButton(
                  onPressed: () {
                    Navigator.of(context).pushNamed('/admin-login');
                  },
                  child: Text(
                    'Войти как администратор',
                    style: TextStyle(
                      color: AppTheme.textSecondary,
                      fontSize: 14,
                    ),
                  ),
                ),
                const SizedBox(height: 32),
              ],
            ),
          ),
        ),
      ),
    );
  }
}
