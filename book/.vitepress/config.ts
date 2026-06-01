import { defineConfig } from 'vitepress'

export default defineConfig({
  title: "Mini-Minio 开发指南",
  description: "从零构建一个轻量级对象存储系统",
  base: "/mini-minio/",
  themeConfig: {
    nav: [
      { text: '首页', link: '/' },
      { text: '开始阅读', link: '/chapters/01-s3-protocol' }
    ],

    sidebar: [
      {
        text: '核心章节',
        items: [
          { text: '第一章：S3 协议基础', link: '/chapters/01-s3-protocol' },
          { text: '第二章：纠删码实现', link: '/chapters/02-erasure-coding' },
          { text: '第三章：桶操作底层设计', link: '/chapters/03-bucket-operations' },
          { text: '第四章：对象读写全面解析', link: '/chapters/04-object-operations' },
          { text: '第五章：高级二级操作', link: '/chapters/05-secondary-operations' },
          { text: '第六章：分布式架构与共识设计', link: '/chapters/06-distributed-design' },
        ]
      }
    ],
    socialLinks: [
      { icon: 'github', link: 'https://github.com/sanbei101/mini-minio' }
    ]
  }
})