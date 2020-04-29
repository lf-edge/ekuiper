#!/usr/bin/env node

const fs = require('fs')
const path = require('path')

const ZH_CN = path.join(__dirname, '../zh_CN')
const EN_US = path.join(__dirname, '../en_US')

// [[match: regexp | fn | string, replace: regexp fn| string]]
const replaceRule = [
  [
    '[English](README.md) | [简体中文](README-CN.md)',
    ''
  ],
  [
    '![arch](docs/resources/arch.png)',
    '![arch](../resources/arch.png)'
  ],
  [
    /]\(docs\/zh_CN\//gim,
    '](./'
  ],
  [
    /]\(docs\/en_US\//gim,
    '](./'
  ],
  [
    '(fvt_scripts/edgex/benchmark/pub.go)',
    'https://github.com/emqx/kuiper/blob/master/fvt_scripts/edgex/pub.go'
  ],
  [
    '[Apache 2.0](LICENSE)',
    '[Apache 2.0](https://github.com/emqx/kuiper/blob/master/LICENSE)'
  ]
]

const readmeMoveRule = [
  {
    from: path.join(__dirname, '../../README.md'),
    to: path.join(EN_US, './README.md'),
  },

  {
    from: path.join(__dirname, '../../README-CN.md'),
    to: path.join(ZH_CN, './README.md'),
  }
]


function generateReadme() {
  readmeMoveRule.forEach(fileInfo => {
    const { from, to } = fileInfo
    // read
    let content = fs.readFileSync(from).toString()

    replaceRule.forEach(rule => {
      content = content.replace(rule[0], rule[1])
    })

    fs.writeFileSync(
      to,
      content,
    )
    console.log(`move ${from} to ${to}`)
  })
}

generateReadme()