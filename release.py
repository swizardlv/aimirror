#!/usr/bin/env python3
"""
aimirror 一键发布脚本
用法: python release.py <版本号> "<更新说明>"
示例: python release.py 0.2.2 "修复bug，优化性能"
"""

import sys
import re
import subprocess
import os


def run_command(cmd, check=True):
    """执行shell命令"""
    print(f"\n>>> {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    if check and result.returncode != 0:
        print(f"命令执行失败: {cmd}")
        sys.exit(1)
    return result


def validate_version(version):
    """验证版本号格式"""
    pattern = r'^\d+\.\d+\.\d+$'
    if not re.match(pattern, version):
        print(f"错误: 版本号格式不正确 '{version}'，应为 x.y.z 格式")
        sys.exit(1)


def update_pyproject(version):
    """更新 pyproject.toml 中的版本号"""
    print(f"\n[1/6] 更新版本号到 {version}...")
    
    with open('pyproject.toml', 'r') as f:
        content = f.read()
    
    # 替换版本号
    new_content = re.sub(
        r'^version = "[^"]+"',
        f'version = "{version}"',
        content,
        flags=re.MULTILINE
    )
    
    with open('pyproject.toml', 'w') as f:
        f.write(new_content)
    
    print(f"✓ pyproject.toml 已更新")


def commit_and_push(version, release_notes):
    """提交代码并推送到GitHub"""
    print(f"\n[2/6] 提交代码到 GitHub...")
    
    run_command("git add -A")
    run_command(f'git commit -m "chore(release): bump version to {version}"')
    run_command("git push origin main")
    
    print("✓ 代码已提交并推送")


def create_and_push_tag(version):
    """创建并推送标签"""
    print(f"\n[3/6] 创建标签 v{version}...")
    
    run_command(f"git tag v{version}")
    run_command(f"git push origin v{version}")
    
    print(f"✓ 标签 v{version} 已创建并推送")


def create_github_release(version, release_notes):
    """创建 GitHub Release"""
    print(f"\n[4/6] 创建 GitHub Release...")
    
    notes = release_notes if release_notes else f"Release version {version}"
    run_command(f'gh release create v{version} --title "v{version}" --notes "{notes}"')
    
    print(f"✓ GitHub Release 已创建")


def build_package():
    """构建 PyPI 包"""
    print(f"\n[5/6] 构建 PyPI 包...")
    
    # 清理旧的构建文件
    run_command("rm -rf dist/ build/ *.egg-info/")
    
    # 构建包
    result = run_command("python3 -m build --no-isolation", check=False)
    if result.returncode != 0:
        print("构建失败，尝试安装依赖后重试...")
        run_command("pip install 'setuptools>=61.0' wheel -i https://pypi.org/simple -q")
        run_command("python3 -m build --no-isolation")
    
    print("✓ PyPI 包构建完成")


def upload_to_pypi():
    """上传到 PyPI"""
    print(f"\n[6/6] 上传到 PyPI...")
    
    run_command("twine upload --config-file .pypirc dist/*")
    
    print("✓ 已上传到 PyPI")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    version = sys.argv[1]
    release_notes = sys.argv[2] if len(sys.argv) > 2 else ""
    
    # 验证版本号
    validate_version(version)
    
    print(f"=" * 50)
    print(f"开始发布 aimirror v{version}")
    print(f"=" * 50)
    
    # 检查必要文件
    if not os.path.exists('.pypirc'):
        print("错误: 未找到 .pypirc 文件，请确保 PyPI 认证配置正确")
        sys.exit(1)
    
    if not os.path.exists('pyproject.toml'):
        print("错误: 未找到 pyproject.toml 文件")
        sys.exit(1)
    
    # 执行发布流程
    try:
        update_pyproject(version)
        commit_and_push(version, release_notes)
        create_and_push_tag(version)
        create_github_release(version, release_notes)
        build_package()
        upload_to_pypi()
        
        print(f"\n" + "=" * 50)
        print(f"🎉 aimirror v{version} 发布成功!")
        print(f"=" * 50)
        print(f"\n发布链接:")
        print(f"  GitHub Release: https://github.com/livehl/aimirror/releases/tag/v{version}")
        print(f"  PyPI: https://pypi.org/project/aimirror/{version}/")
        print(f"\n安装命令:")
        print(f"  pip install aimirror=={version}")
        
    except Exception as e:
        print(f"\n❌ 发布失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
