import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'providers/app_provider.dart';
import 'theme/app_theme.dart';
import 'screens/onboarding/onboarding_screen.dart';
import 'screens/schedule/schedule_screen.dart';
import 'screens/admin/admin_login_screen.dart';
import 'screens/admin/admin_panel_screen.dart';

class UniScheduleApp extends StatelessWidget {
  const UniScheduleApp({super.key});

  @override
  Widget build(BuildContext context) {
    return Consumer<AppProvider>(
      builder: (context, provider, _) {
        return MaterialApp(
          title: 'UniSchedule',
          debugShowCheckedModeBanner: false,
          theme: AppTheme.lightTheme,
          darkTheme: AppTheme.darkTheme,
          themeMode: ThemeMode.system,

          // Start on onboarding or schedule based on state
          initialRoute: provider.onboardingComplete ? '/schedule' : '/onboarding',

          routes: {
            '/onboarding': (_) => const OnboardingScreen(),
            '/schedule': (_) => const ScheduleScreen(),
            '/admin-login': (_) => const AdminLoginScreen(),
            '/admin': (_) => const AdminPanelScreen(),
          },
        );
      },
    );
  }
}
