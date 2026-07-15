'''Reusable repository style checks for Python projects.'''

from __future__ import annotations

import ast
import io
import re
import sys
import tokenize
from dataclasses import dataclass
from pathlib import Path


MAX_LINE_LENGTH = 90
TARGET_LINE_LENGTH = 80
TOP_PARAM_BLOCK_CANDIDATES = 5
DEFAULT_SKIP_PARTS = {
    '.git',
    '.mypy_cache',
    '.pytest_cache',
    '.ruff_cache',
    '.tox',
    '.venv',
    '__pycache__',
    'build',
    'dist',
    'node_modules',
    'venv',
}
DEFAULT_ROOTS = ('src', 'app', 'lib')
WRAPPED_IMPORT_RE = re.compile(r'^\s*from\s+\S+\s+import\s*\(')
ARG_DOC_RE = re.compile(r'^([a-zA-Z_][a-zA-Z0-9_]*):\s{2,}\S')
DOCSTRING_EXEMPTIONS = set()


@dataclass
class Finding:
    path: Path
    line: int
    level: str
    message: str

    def format(self) -> str:
        return f'{self.level}: {self.path}:{self.line}: {self.message}'


def main(argv=None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    findings = []
    for path in iter_python_files(argv):
        source = path.read_text()
        findings.extend(check_line_lengths(path, source))
        findings.extend(check_wrapped_imports(path, source))
        findings.extend(check_quote_style(path, source))
        findings.extend(check_ast_rules(path, source))

    findings.sort(key=lambda item: (str(item.path), item.line, item.level))
    for finding in findings:
        print(finding.format())

    error_count = sum(f.level == 'ERROR' for f in findings)
    warning_count = sum(f.level == 'WARN' for f in findings)
    if findings:
        print(f'{error_count} error(s), {warning_count} warning(s)')
    return 1 if error_count else 0


def iter_python_files(targets):
    seen = set()
    if targets:
        for target_text in targets:
            target = Path(target_text)
            if not target.exists():
                continue
            yield from iter_target_python_files(target, seen)
        return

    roots = [Path(name) for name in DEFAULT_ROOTS if Path(name).exists()]
    if roots:
        for root in roots:
            yield from iter_target_python_files(root, seen)
        return

    yield from iter_target_python_files(Path('.'), seen)


def iter_target_python_files(target, seen):
    if target.is_file():
        if target.suffix == '.py' and target not in seen:
            seen.add(target)
            yield target
        return

    for path in sorted(target.rglob('*.py')):
        if should_skip_path(path):
            continue
        if path in seen:
            continue
        seen.add(path)
        yield path


def should_skip_path(path):
    return any(part in DEFAULT_SKIP_PARTS for part in path.parts)


def check_line_lengths(path, source):
    findings = []
    for lineno, line in enumerate(source.splitlines(), start=1):
        line_length = len(line)
        if line_length > MAX_LINE_LENGTH:
            message = f'line too long ({line_length} > {MAX_LINE_LENGTH})'
            findings.append(Finding(path, lineno, 'ERROR', message))
        elif line_length > TARGET_LINE_LENGTH:
            message = f'line exceeds target ({line_length} > {TARGET_LINE_LENGTH})'
            findings.append(Finding(path, lineno, 'WARN', message))
    return findings


def check_wrapped_imports(path, source):
    findings = []
    for lineno, line in enumerate(source.splitlines(), start=1):
        if WRAPPED_IMPORT_RE.match(line):
            findings.append(Finding(path, lineno, 'ERROR',
                'wrapped from-import with parentheses is not allowed'))
    return findings


def check_quote_style(path, source):
    findings = []
    try:
        tokens = tokenize.generate_tokens(io.StringIO(source).readline)
    except tokenize.TokenError:
        return findings
    for token in tokens:
        if token.type != tokenize.STRING:
            continue
        token_text = token.string
        if _is_docstring_candidate(token_text):
            continue
        if _uses_double_quotes_without_need(token_text):
            findings.append(Finding(path, token.start[0], 'ERROR',
                'use single quotes unless double quotes are needed'))
    return findings


def check_ast_rules(path, source):
    findings = []
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        findings.append(Finding(path, exc.lineno or 1, 'ERROR',
            f'syntax error: {exc.msg}'))
        return findings
    findings.extend(check_module_length(path, source, tree))
    findings.extend(check_module_function_order(path, tree))
    findings.extend(check_class_method_order(path, tree))
    findings.extend(check_public_docstrings(path, tree))
    findings.extend(check_small_multiline_dict_literals(path, source, tree))
    findings.extend(check_compactable_simple_blocks(path, source, tree))
    findings.extend(check_direct_nested_calls(path, source, tree))
    findings.extend(check_multiline_inline_raise(path, source, tree))
    findings.extend(check_compactable_multiline_dict_literals(path, source,
        tree))
    findings.extend(check_multiline_comprehensions(path, tree))
    findings.extend(check_vararg_usage(path, tree))
    findings.extend(check_docstring_blocks(path, tree))
    return findings


def check_module_length(path, source, tree):
    findings = []
    total_lines = len(source.splitlines())
    if total_lines <= 600:
        return findings

    top_level_defs = [node for node in tree.body if isinstance(node,
        (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))]
    top_level_classes = [node for node in tree.body if isinstance(node,
        ast.ClassDef)]
    public_top_level_functions = [node for node in tree.body if isinstance(node,
        (ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith('_')]

    if total_lines <= 700:
        if len(top_level_classes) <= 1 and len(public_top_level_functions) <= 2:
            return findings

    message = module_length_message(total_lines, len(top_level_defs),
        len(top_level_classes), len(public_top_level_functions))
    findings.append(Finding(path, 1, 'WARN', message))
    return findings


def check_module_function_order(path, tree):
    findings = []
    seen_private = False
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name.startswith('_'):
            seen_private = True
            continue
        if seen_private:
            findings.append(Finding(path, node.lineno, 'WARN',
                'agent review: check ordering first; keep important public '
                'functions before helpers'))
    return findings


def check_class_method_order(path, tree):
    findings = []
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        stage = 'dunder'
        for item in node.body:
            if not isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            name = item.name
            if _is_dunder(name):
                if stage != 'dunder':
                    findings.append(Finding(path, item.lineno, 'WARN',
                        f'agent review: check ordering first; keep dunder '
                        f'method {name} before other methods'))
                continue
            if name.startswith('_'):
                stage = 'private'
                continue
            if stage == 'private':
                findings.append(Finding(path, item.lineno, 'WARN',
                    f'agent review: check ordering first; keep important '
                    f'public method {name} before private helpers'))
            stage = 'public'
    return findings


def check_public_docstrings(path, tree):
    findings = []
    module_name = python_module_name(path)
    public_functions = []
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            findings.extend(check_public_docstrings_in_class(path,
                module_name, node))
            continue
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name.startswith('_'):
            continue
        public_functions.append(node)

    top_param_candidates = top_param_block_candidates(public_functions)
    for node in public_functions:
        if is_docstring_exempt(module_name, node.name):
            continue
        docstring = ast.get_docstring(node, clean=False)
        if docstring is None:
            findings.append(Finding(path, node.lineno, 'WARN',
                f'agent review: if {node.name} stays in the main public API, '
                'add a docstring'))
            continue
        if node.name in top_param_candidates and has_two_or_more_params(node):
            if not has_parameter_block(docstring):
                findings.append(Finding(path, node.lineno, 'WARN',
                    f'agent review: after checking ordering, add a parameter '
                    f'block if {node.name} stays in the early public API'))
    return findings


def check_public_docstrings_in_class(path, module_name, class_node):
    findings = []
    public_methods = []
    for node in class_node.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name != '__init__' and node.name.startswith('_'):
            continue
        public_methods.append(node)

    top_param_candidates = top_param_block_candidates(public_methods)
    for node in public_methods:
        if is_docstring_exempt(module_name, class_node.name, node.name):
            continue
        docstring = ast.get_docstring(node, clean=False)
        if docstring is None:
            message = f'agent review: add a docstring if '
            message += f'{class_node.name}.{node.name} stays in the main '
            message += 'public API'
            findings.append(Finding(path, node.lineno, 'WARN', message))
            continue
        if node.name in top_param_candidates and has_two_or_more_params(node):
            if has_parameter_block(docstring):
                continue
            message = 'agent review: after checking ordering, add a '
            message += 'parameter block if '
            message += f'{class_node.name}.{node.name} stays in the early '
            message += 'public API'
            findings.append(Finding(path, node.lineno, 'WARN', message))
    return findings


def check_vararg_usage(path, tree):
    findings = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.args.vararg is not None:
            findings.append(Finding(path, node.lineno, 'WARN',
                f'agent review: {node.name} uses *args; keep only with strong '
                'justification'))
        if node.args.kwarg is not None:
            findings.append(Finding(path, node.lineno, 'WARN',
                f'agent review: {node.name} uses **kwargs; keep only with '
                'strong justification'))
    return findings


def check_small_multiline_dict_literals(path, source, tree):
    findings = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        if node.lineno == getattr(node, 'end_lineno', node.lineno):
            continue
        item_count = len(node.keys)
        if item_count > 3:
            continue
        source_text = ast.get_source_segment(source, node)
        if source_text is not None:
            compact = ' '.join(source_text.split())
            if len(compact) > MAX_LINE_LENGTH:
                continue
        findings.append(Finding(path, node.lineno, 'ERROR',
            'small dict literals must stay on one line'))
    return findings


def check_compactable_simple_blocks(path, source, tree):
    findings = []
    lines = source.splitlines()
    for node in ast.walk(tree):
        if isinstance(node, ast.If):
            findings.extend(check_short_if_terminal(path, lines, node))
        if isinstance(node, (ast.For, ast.AsyncFor, ast.While)):
            findings.extend(check_loop_single_if(path, lines, node))
    return findings


def check_direct_nested_calls(path, source, tree):
    findings = []
    parent_map = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parent_map[child] = parent
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not is_direct_nested_call(node):
            continue
        if has_nested_call_ancestor(node, parent_map):
            continue
        source_text = ast.get_source_segment(source, node)
        if source_text is None:
            continue
        compact = ' '.join(source_text.split())
        is_multiline = '\n' in source_text
        depth = call_nesting_depth(node)
        if not is_multiline and depth < 3 and len(compact) <= 40:
            continue
        multiline_text = 'multiline' if is_multiline else 'single-line'
        message = 'separate nested calls into named steps '
        message += f'(depth={depth}, {multiline_text}, '
        message += f'compact_len={len(compact)})'
        findings.append(Finding(path, node.lineno, 'ERROR', message))
    return findings


def check_multiline_inline_raise(path, source, tree):
    findings = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Raise):
            continue
        exc = node.exc
        if not isinstance(exc, ast.Call):
            continue
        if exc.lineno == getattr(exc, 'end_lineno', exc.lineno):
            continue
        if not has_inline_string_argument(exc):
            continue
        findings.append(Finding(path, node.lineno, 'ERROR',
            'build exception messages in a variable before multi-line raise'))
    return findings


def check_compactable_multiline_dict_literals(path, source, tree):
    findings = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        if node.lineno == getattr(node, 'end_lineno', node.lineno):
            continue
        item_count = len(node.keys)
        if item_count <= 3 or item_count > 5:
            continue
        source_text = ast.get_source_segment(source, node)
        if source_text is None:
            continue
        compact = ' '.join(source_text.split())
        if len(compact) > MAX_LINE_LENGTH:
            continue
        if not has_one_item_per_line_layout(source_text):
            continue
        message = 'dict literal is vertically expanded; keep it more compact '
        message += 'when it still fits cleanly on one line'
        findings.append(Finding(path, node.lineno, 'WARN', message))
    return findings


def check_multiline_comprehensions(path, tree):
    findings = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.DictComp, ast.ListComp, ast.SetComp,
                ast.GeneratorExp)):
            continue
        if node.lineno == getattr(node, 'end_lineno', node.lineno):
            continue
        findings.append(Finding(path, node.lineno, 'WARN',
            'prefer explicit loops over multi-line comprehensions'))
    return findings


def check_docstring_blocks(path, tree):
    findings = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if node.name.startswith('_'):
            continue
        docstring = ast.get_docstring(node, clean=False)
        if not docstring or '\n' not in docstring:
            continue
        lines = [line.rstrip() for line in docstring.splitlines()[1:]
            if line.strip()]
        arg_lines = [line for line in lines if ':' in line]
        if not arg_lines:
            continue
        if any(not ARG_DOC_RE.match(line.strip()) for line in arg_lines):
            findings.append(Finding(path, node.lineno, 'WARN',
                f'docstring parameter block for {node.name} is malformed'))
    return findings


def check_block_suite(path, lines, node, suite, header_kind='main',
    predicate=None):
    findings = []
    if not isinstance(suite, list) or not suite or len(suite) != 1:
        return findings
    stmt = suite[0]
    if predicate is not None:
        if not predicate(stmt):
            return findings
    elif not is_simple_inline_statement(stmt):
        return findings
    header_lineno = suite_header_lineno(node, stmt, header_kind)
    if header_lineno is None or header_lineno < 1 or header_lineno > len(lines):
        return findings
    header_text = lines[header_lineno - 1].strip()
    if not header_text.endswith(':'):
        return findings
    statement_text = lines[stmt.lineno - 1].strip()
    if not statement_text:
        return findings
    combined = header_text + ' ' + statement_text
    if len(combined) > TARGET_LINE_LENGTH:
        return findings
    findings.append(Finding(path, header_lineno, 'ERROR',
        'short simple block should stay on one line'))
    return findings


def check_short_if_terminal(path, lines, node):
    terminal_nodes = (ast.Return, ast.Raise, ast.Break, ast.Continue, ast.Pass)
    findings = []
    findings.extend(check_block_suite(path, lines, node, node.body,
        predicate=lambda stmt: isinstance(stmt, terminal_nodes)))
    if node.orelse and not isinstance(node.orelse[0], ast.If):
        findings.extend(check_block_suite(path, lines, node, node.orelse,
            header_kind='else',
            predicate=lambda stmt: isinstance(stmt, terminal_nodes)))
    return findings


def check_loop_single_if(path, lines, node):
    if not isinstance(node.body, list) or len(node.body) != 1:
        return []
    inner_if = node.body[0]
    if not is_short_if_simple_stmt(inner_if):
        return []
    return check_block_suite(path, lines, inner_if, inner_if.body,
        predicate=is_loop_inline_statement)


def suite_header_lineno(node, stmt, header_kind):
    if header_kind == 'main':
        return getattr(node, 'lineno', None)
    header_line = stmt.lineno - 1
    if header_kind == 'else':
        return header_line
    if header_kind == 'finally':
        return header_line
    return header_line


def is_simple_inline_statement(node):
    simple_nodes = (ast.Assign, ast.AugAssign, ast.AnnAssign, ast.Expr,
        ast.Return, ast.Raise, ast.Pass, ast.Break, ast.Continue, ast.Import,
        ast.ImportFrom)
    return isinstance(node, simple_nodes)


def module_length_message(total_lines, top_level_defs, top_level_classes,
    public_top_level_functions):
    message = f'large module ({total_lines} lines); '
    if total_lines >= 1000:
        message += 'strongly consider splitting code into helper modules or '
        message += 'moving secondary responsibilities out'
    elif total_lines >= 800:
        message += 'consider splitting code into helper modules or moving '
        message += 'secondary responsibilities out'
    else:
        message += 'review whether some code should move to helper modules or '
        message += 'whether the module would benefit from splitting'
    message += f' (top_level_defs={top_level_defs}, '
    message += f'classes={top_level_classes}, '
    message += f'public_functions={public_top_level_functions})'
    return message


def top_param_block_candidates(nodes):
    candidates = []
    for node in nodes:
        if has_two_or_more_params(node):
            candidates.append(node.name)
        if len(candidates) >= TOP_PARAM_BLOCK_CANDIDATES:
            break
    return set(candidates)


def has_two_or_more_params(node):
    parameter_count = len(real_parameters(node))
    return parameter_count >= 2


def is_direct_nested_call(node):
    if isinstance(node.func, ast.Call):
        return True
    for arg in node.args:
        if isinstance(arg, ast.Call):
            return True
    for keyword in node.keywords:
        if isinstance(keyword.value, ast.Call):
            return True
    return False


def call_nesting_depth(node):
    depth = 1
    child_calls = []
    if isinstance(node.func, ast.Call):
        child_calls.append(node.func)
    for arg in node.args:
        if isinstance(arg, ast.Call):
            child_calls.append(arg)
    for keyword in node.keywords:
        if isinstance(keyword.value, ast.Call):
            child_calls.append(keyword.value)
    if not child_calls:
        return depth
    return depth + max(call_nesting_depth(child) for child in child_calls)


def has_nested_call_ancestor(node, parent_map):
    current = parent_map.get(node)
    while current is not None:
        if isinstance(current, ast.Call) and is_direct_nested_call(current):
            return True
        current = parent_map.get(current)
    return False


def has_inline_string_argument(node):
    for arg in node.args:
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            return True
        if isinstance(arg, ast.JoinedStr):
            return True
    return False


def has_one_item_per_line_layout(source_text):
    lines = [line.strip() for line in source_text.splitlines()]
    item_lines = []
    for line in lines[1:-1]:
        if not line or line.startswith('#'):
            continue
        item_lines.append(line)
    if len(item_lines) < 2:
        return False
    return all(line.endswith(',') for line in item_lines)


def is_short_if_simple_stmt(node):
    if not isinstance(node, ast.If):
        return False
    if node.orelse:
        return False
    simple_nodes = (ast.Assign, ast.AugAssign, ast.AnnAssign, ast.Expr,
        ast.Return, ast.Raise, ast.Pass, ast.Break, ast.Continue)
    return len(node.body) == 1 and isinstance(node.body[0], simple_nodes)


def is_loop_inline_statement(node):
    simple_nodes = (ast.Assign, ast.AugAssign, ast.AnnAssign, ast.Expr,
        ast.Return, ast.Raise, ast.Pass, ast.Break, ast.Continue)
    return isinstance(node, simple_nodes)


def _is_docstring_candidate(token_text):
    prefixes = ('"""', "'''", 'r"""', "r'''", 'u"""', "u'''",
        'f"""', "f'''", 'fr"""', "fr'''", 'rf"""', "rf'''")
    lowered = token_text.lower()
    return lowered.startswith(prefixes)


def _uses_double_quotes_without_need(token_text):
    prefix = ''
    while token_text and token_text[0] in 'rRuUbBfF':
        prefix += token_text[0]
        token_text = token_text[1:]
    if not token_text.startswith('"') or token_text.startswith('"""'):
        return False
    if "'" in token_text and '"' not in token_text[1:-1]:
        return False
    body = token_text[1:-1]
    if "'" in body:
        return False
    return True


def _is_dunder(name):
    return name.startswith('__') and name.endswith('__')


def has_parameter_block(docstring):
    if '\n' not in docstring:
        return False
    lines = [line.strip() for line in docstring.splitlines()[1:] if line.strip()]
    return any(ARG_DOC_RE.match(line) for line in lines)


def is_docstring_exempt(module_name, function_name, method_name=None):
    if method_name is None:
        key = f'{module_name}/{function_name}'
    else:
        key = f'{module_name}/{function_name}/{method_name}'
    return key in DOCSTRING_EXEMPTIONS


def python_module_name(path):
    parts = list(path.with_suffix('').parts)
    return '.'.join(parts)


def real_parameters(node):
    parameters = []
    posonlyargs = getattr(node.args, 'posonlyargs', [])
    for arg in posonlyargs + node.args.args + node.args.kwonlyargs:
        if arg.arg in ('self', 'cls'):
            continue
        parameters.append(arg)
    return parameters


if __name__ == '__main__':
    raise SystemExit(main())
