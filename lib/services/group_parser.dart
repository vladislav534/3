/// Сервис для парсинга и нормализации названий групп.
///
/// Поддерживает форматы:
/// - эн251, ЭН251, Эн251
/// - эн-251, ЭН-251, Эн-251
/// - эн 251, ЭН 251
/// - эн.251
/// - любые комбинации регистров и разделителей
class GroupParser {
  static final RegExp _groupPattern = RegExp(
    r'^([а-яА-ЯёЁa-zA-Z]{1,5})\s*[-.\s]?\s*(\d{2,4})$',
  );

  /// Нормализует введённую группу в формат "ЭН-251"
  static String? normalize(String input) {
    final trimmed = input.trim();
    if (trimmed.isEmpty) return null;

    final match = _groupPattern.firstMatch(trimmed);
    if (match == null) return null;

    final letters = match.group(1)!.toUpperCase();
    final digits = match.group(2)!;

    return '$letters-$digits';
  }

  /// Проверяет, является ли введённая строка валидной группой
  static bool isValid(String input) {
    return normalize(input) != null;
  }

  /// Извлекает буквенную часть группы
  static String? getPrefix(String input) {
    final normalized = normalize(input);
    if (normalized == null) return null;
    return normalized.split('-').first;
  }

  /// Извлекает числовую часть группы
  static String? getNumber(String input) {
    final normalized = normalize(input);
    if (normalized == null) return null;
    return normalized.split('-').last;
  }

  /// Проверяет, совпадают ли две группы (с учётом разных форматов ввода)
  static bool areEqual(String group1, String group2) {
    final n1 = normalize(group1);
    final n2 = normalize(group2);
    if (n1 == null || n2 == null) return false;
    return n1 == n2;
  }
}
