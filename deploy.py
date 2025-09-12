#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动化部署脚本
用于将项目部署到Railway平台
"""

import os
import sys
import subprocess
import json
from pathlib import Path

def run_command(command, cwd=None):
    """执行命令并返回结果"""
    try:
        result = subprocess.run(
            command,
            shell=True,
            cwd=cwd,
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def check_git_status():
    """检查Git状态"""
    print("检查Git状态...")
    
    # 检查是否在Git仓库中
    success, stdout, stderr = run_command("git status")
    if not success:
        print("❌ 当前目录不是Git仓库，正在初始化...")
        success, stdout, stderr = run_command("git init")
        if not success:
            print(f"❌ Git初始化失败: {stderr}")
            return False
        print("✅ Git仓库初始化成功")
    else:
        print("✅ 已在Git仓库中")
    
    return True

def check_files():
    """检查必要文件是否存在"""
    print("检查必要文件...")
    
    required_files = [
        'app.py',
        'requirements.txt',
        'Procfile',
        'railway.json',
        'runtime.txt',
        '.gitignore',
        'README.md'
    ]
    
    missing_files = []
    for file in required_files:
        if not Path(file).exists():
            missing_files.append(file)
    
    if missing_files:
        print(f"❌ 缺少必要文件: {', '.join(missing_files)}")
        return False
    
    print("✅ 所有必要文件都存在")
    return True

def validate_config():
    """验证配置文件"""
    print("验证配置文件...")
    
    # 检查railway.json
    try:
        with open('railway.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        print("✅ railway.json 格式正确")
    except Exception as e:
        print(f"❌ railway.json 格式错误: {e}")
        return False
    
    # 检查requirements.txt
    try:
        with open('requirements.txt', 'r', encoding='utf-8') as f:
            requirements = f.read().strip()
        if not requirements:
            print("❌ requirements.txt 为空")
            return False
        print("✅ requirements.txt 内容正确")
    except Exception as e:
        print(f"❌ 读取requirements.txt失败: {e}")
        return False
    
    # 检查Procfile
    try:
        with open('Procfile', 'r', encoding='utf-8') as f:
            procfile = f.read().strip()
        if not procfile.startswith('web:'):
            print("❌ Procfile 格式错误")
            return False
        print("✅ Procfile 格式正确")
    except Exception as e:
        print(f"❌ 读取Procfile失败: {e}")
        return False
    
    return True

def commit_changes():
    """提交更改到Git"""
    print("提交更改到Git...")
    
    # 添加所有文件
    success, stdout, stderr = run_command("git add .")
    if not success:
        print(f"❌ 添加文件失败: {stderr}")
        return False
    
    # 检查是否有更改
    success, stdout, stderr = run_command("git diff --cached --quiet")
    if success:
        print("ℹ️ 没有新的更改需要提交")
        return True
    
    # 提交更改
    commit_message = "Deploy: Update crypto pattern monitor for Railway"
    success, stdout, stderr = run_command(f'git commit -m "{commit_message}"')
    if not success:
        print(f"❌ 提交失败: {stderr}")
        return False
    
    print("✅ 更改已提交")
    return True

def setup_remote_repository():
    """设置远程仓库"""
    print("设置远程仓库...")
    
    # 检查是否已有远程仓库
    success, stdout, stderr = run_command("git remote -v")
    if success and 'origin' in stdout:
        print("✅ 远程仓库已配置")
        return True
    
    # 提示用户添加远程仓库
    print("\n⚠️ 需要手动添加远程仓库:")
    print("1. 在GitHub上创建新仓库 'crypto-monitor-railway912'")
    print("2. 执行以下命令:")
    print("   git remote add origin https://github.com/yourusername/crypto-monitor-railway912.git")
    print("   git branch -M main")
    print("   git push -u origin main")
    print("\n按Enter键继续...")
    input()
    
    return True

def push_to_repository():
    """推送到远程仓库"""
    print("推送到远程仓库...")
    
    # 检查远程仓库
    success, stdout, stderr = run_command("git remote -v")
    if not success or 'origin' not in stdout:
        print("❌ 未配置远程仓库")
        return False
    
    # 推送到远程仓库
    success, stdout, stderr = run_command("git push origin main")
    if not success:
        print(f"❌ 推送失败: {stderr}")
        print("\n可能的解决方案:")
        print("1. 检查GitHub仓库是否存在")
        print("2. 检查网络连接")
        print("3. 检查Git凭据")
        return False
    
    print("✅ 代码已推送到远程仓库")
    return True

def show_deployment_guide():
    """显示部署指南"""
    print("\n" + "="*60)
    print("🚀 Railway部署指南")
    print("="*60)
    print("\n1. 访问 https://railway.app")
    print("2. 使用GitHub账户登录")
    print("3. 点击 'New Project'")
    print("4. 选择 'Deploy from GitHub repo'")
    print("5. 选择 'crypto-monitor-railway912' 仓库")
    print("6. Railway会自动检测配置并开始部署")
    print("\n部署完成后:")
    print("- 访问 https://your-app.railway.app/ 进行健康检查")
    print("- 访问 https://your-app.railway.app/status 查看系统状态")
    print("\n可选配置:")
    print("- 在Railway项目设置中添加环境变量 WEBHOOK_URL")
    print("- 监控部署日志确保服务正常运行")
    print("\n" + "="*60)

def test_local_app():
    """测试本地应用"""
    print("测试本地应用...")
    
    try:
        # 简单的语法检查
        import ast
        with open('app.py', 'r', encoding='utf-8') as f:
            code = f.read()
        ast.parse(code)
        print("✅ app.py 语法检查通过")
        
        # 检查导入
        import importlib.util
        spec = importlib.util.spec_from_file_location("app", "app.py")
        if spec is None:
            print("❌ 无法加载app.py")
            return False
        
        print("✅ 应用文件检查通过")
        return True
        
    except SyntaxError as e:
        print(f"❌ app.py 语法错误: {e}")
        return False
    except Exception as e:
        print(f"❌ 应用检查失败: {e}")
        return False

def main():
    """主函数"""
    print("🚀 开始自动化部署流程")
    print("="*50)
    
    # 检查当前目录
    current_dir = Path.cwd()
    print(f"当前目录: {current_dir}")
    
    # 执行检查步骤
    steps = [
        ("检查必要文件", check_files),
        ("验证配置文件", validate_config),
        ("测试本地应用", test_local_app),
        ("检查Git状态", check_git_status),
        ("提交更改", commit_changes),
        ("设置远程仓库", setup_remote_repository),
        ("推送到仓库", push_to_repository)
    ]
    
    for step_name, step_func in steps:
        print(f"\n📋 {step_name}")
        print("-" * 30)
        
        if not step_func():
            print(f"\n❌ {step_name} 失败，部署中止")
            sys.exit(1)
    
    print("\n✅ 所有检查通过！")
    show_deployment_guide()
    
    print("\n🎉 部署准备完成！")
    print("现在可以在Railway上部署你的应用了。")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ 部署流程被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ 部署流程出错: {e}")
        sys.exit(1)