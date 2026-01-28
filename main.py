#!/usr/bin/env python3
"""
Alteryx to DBT Documentation Generator

A tool for parsing Alteryx workflows and generating documentation
to facilitate migration to Trino/DBT ELT architecture.

Usage:
    python main.py analyze <path> [options]
    python main.py --help

Examples:
    python main.py analyze .                          # Analyze current directory
    python main.py analyze ./workflows --recursive    # Analyze recursively
    python main.py analyze . --output ./docs          # Specify output directory
    python main.py analyze . --generate-dbt ./dbt     # Generate DBT scaffolding
    python main.py analyze . --macro-dir ./macros     # Specify macro directory
"""
import argparse
import sys
from pathlib import Path
from typing import List, Optional

from alteryx_parser import AlteryxParser, parse_workflow
from macro_handler import MacroResolver, MacroInventory
from doc_generator import DocumentationGenerator
from dbt_generator import DBTGenerator
from models import AlteryxWorkflow


def find_workflows(path: Path, recursive: bool = False) -> List[Path]:
    """Find all Alteryx workflow files in a path."""
    workflows = []

    if path.is_file():
        if path.suffix.lower() in ['.yxmd', '.yxmc', '.yxwz']:
            workflows.append(path)
    elif path.is_dir():
        pattern = '**/*.yxmd' if recursive else '*.yxmd'
        workflows.extend(path.glob(pattern))

        # Also find .yxmc (macro) files that might be standalone
        macro_pattern = '**/*.yxmc' if recursive else '*.yxmc'
        workflows.extend(path.glob(macro_pattern))

    return sorted(workflows)


def analyze(args) -> int:
    """Main analyze command."""
    target_path = Path(args.path).resolve()

    if not target_path.exists():
        print(f"Error: Path does not exist: {target_path}")
        return 1

    # Find workflows
    print(f"Scanning for Alteryx workflows in: {target_path}")
    workflow_files = find_workflows(target_path, args.recursive)

    if not workflow_files:
        print("No Alteryx workflow files (.yxmd) found.")
        return 1

    print(f"Found {len(workflow_files)} workflow file(s)")

    # Setup macro resolver
    macro_resolver = MacroResolver(
        interactive=not args.non_interactive,
        skip_all=args.non_interactive,
    )

    # Add macro directories
    for macro_dir in args.macro_dir or []:
        macro_resolver.add_search_directory(macro_dir)

    # Parse workflows
    workflows: List[AlteryxWorkflow] = []
    macro_inventory = MacroInventory()
    parser = AlteryxParser()

    for wf_path in workflow_files:
        print(f"\nParsing: {wf_path.name}")
        try:
            workflow = parser.parse(str(wf_path))
            workflows.append(workflow)

            print(f"  - {len(workflow.nodes)} tools")
            print(f"  - {len(workflow.sources)} sources")
            print(f"  - {len(workflow.targets)} targets")
            print(f"  - {len(workflow.macros_used)} macros referenced")

            # Resolve macros
            if workflow.macros_used:
                macro_infos = macro_resolver.resolve_macros(workflow)
                for macro_path, macro_info in macro_infos.items():
                    macro_inventory.add_macro(macro_info, workflow.metadata.name)

                if workflow.missing_macros:
                    print(f"  - {len(workflow.missing_macros)} macros not found")

        except Exception as e:
            print(f"  Error parsing {wf_path}: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()

    if not workflows:
        print("\nNo workflows were successfully parsed.")
        return 1

    # Determine output directory
    output_dir = Path(args.output) if args.output else target_path / "alteryx_docs"

    # Generate DBT scaffolding first if requested (to collect TODOs)
    dbt_todos = None
    if args.generate_dbt:
        dbt_dir = Path(args.generate_dbt)
        print(f"\nGenerating DBT project to: {dbt_dir}")
        # Pass interactive flag (inverse of non_interactive)
        dbt_generator = DBTGenerator(
            str(dbt_dir),
            interactive=not args.non_interactive
        )
        # Pass macro_inventory for reusable macro generation
        dbt_generator.generate(workflows, macro_inventory)
        # Collect TODOs for documentation
        dbt_todos = dbt_generator.todos

        # Validate generated SQL if requested (HIGH-05 fix)
        if args.validate:
            print("\nValidating generated SQL...")
            validation_result = dbt_generator.validate_sql()
            if validation_result['success']:
                print(f"  Validation passed: {validation_result['models_validated']} models validated")
            else:
                print(f"  Validation failed: {validation_result['error']}")
                if args.verbose and validation_result.get('details'):
                    print(f"  Details: {validation_result['details']}")

    # Generate documentation (including TODO guide if DBT was generated)
    print(f"\nGenerating documentation to: {output_dir}")
    doc_generator = DocumentationGenerator(str(output_dir))
    doc_generator.generate_all(workflows, macro_inventory, dbt_todos)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Workflows analyzed: {len(workflows)}")
    print(f"Total tools: {sum(len(w.nodes) for w in workflows)}")
    print(f"Total sources: {sum(len(w.sources) for w in workflows)}")
    print(f"Total outputs: {sum(len(w.targets) for w in workflows)}")

    macro_summary = macro_inventory.get_summary()
    print(f"Macros found: {macro_summary['found']}")
    print(f"Macros missing: {macro_summary['missing']}")

    print(f"\nDocumentation: {output_dir / 'index.md'}")
    if args.generate_dbt:
        print(f"DBT Project: {Path(args.generate_dbt) / 'dbt_project.yml'}")

    return 0


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Alteryx to DBT Documentation Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s analyze .                           Analyze workflows in current directory
  %(prog)s analyze ./workflows -r              Analyze recursively
  %(prog)s analyze . -o ./docs                 Output to specific directory
  %(prog)s analyze . --generate-dbt ./dbt      Also generate DBT scaffolding
  %(prog)s analyze . --macro-dir ./macros      Specify macro search directory
  %(prog)s analyze . --non-interactive         Skip prompts for missing macros
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Analyze command
    analyze_parser = subparsers.add_parser(
        'analyze',
        help='Analyze Alteryx workflows and generate documentation'
    )

    analyze_parser.add_argument(
        'path',
        help='Path to workflow file or directory containing workflows'
    )

    analyze_parser.add_argument(
        '-r', '--recursive',
        action='store_true',
        help='Recursively search for workflows in subdirectories'
    )

    analyze_parser.add_argument(
        '-o', '--output',
        help='Output directory for documentation (default: <path>/alteryx_docs)'
    )

    analyze_parser.add_argument(
        '--generate-dbt',
        metavar='DIR',
        help='Also generate DBT project scaffolding to specified directory'
    )

    analyze_parser.add_argument(
        '--macro-dir',
        action='append',
        metavar='DIR',
        help='Directory to search for macros (can be specified multiple times)'
    )

    analyze_parser.add_argument(
        '--non-interactive',
        action='store_true',
        help='Skip interactive prompts for missing macros'
    )

    analyze_parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose output'
    )

    analyze_parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate generated SQL by running dbt compile (requires dbt to be installed)'
    )

    # Parse arguments
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 1

    if args.command == 'analyze':
        return analyze(args)

    return 0


if __name__ == '__main__':
    sys.exit(main())
