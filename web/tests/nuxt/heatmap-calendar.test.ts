import { createRequire } from 'node:module'
import { afterEach, describe, expect, it } from 'vitest'
import { mountSuspended } from '@nuxt/test-utils/runtime'
import { enableAutoUnmount } from '@vue/test-utils'
import { NHeatmap, type HeatmapData } from 'naive-ui'

const { NHeatmap: CommonJsHeatmap } = createRequire(import.meta.url)('naive-ui') as Pick<typeof import('naive-ui'), 'NHeatmap'>
enableAutoUnmount(afterEach)

function days(year: number, month: number, day: number, count: number, unknown = 0): HeatmapData {
  return Array.from({ length: count }, (_, index) => ({
    timestamp: new Date(year, month - 1, day + index).getTime(), value: index < unknown ? null : 0
  }))
}

describe.each([['ES module', NHeatmap], ['CommonJS', CommonJsHeatmap]] as const)('NHeatmap 月份列定位（%s）', (_entry, Heatmap) => {
  async function calendar(data: HeatmapData) {
    const wrapper = await mountSuspended(Heatmap, { props: { data, firstDayOfWeek: 0, tooltip: false } })
    let column = 0
    const headers = wrapper.findAll('thead th[colspan]').map(cell => {
      const span = Number(cell.attributes('colspan'))
      const result = { name: cell.text(), start: column, span }
      column += span
      return result
    })
    const bodyColumns = wrapper.get('tbody tr').findAll('.n-heatmap__day-cell').length
    expect(column).toBe(bodyColumns)
    return { wrapper, headers, bodyColumns }
  }

  it('不足三周的首月保留空白表头宽度，十一月从正确周列开始', async () => {
    // 周一日历：2025-10-20、10-27 后才是 11-03；末列为 2026-10-19。
    const { headers, bodyColumns } = await calendar(days(2025, 10, 25, 365))
    expect(bodyColumns).toBe(53)
    expect(headers).toEqual([
      { name: '', start: 0, span: 2 }, { name: 'Nov', start: 2, span: 4 },
      { name: 'Dec', start: 6, span: 5 }, { name: 'Jan', start: 11, span: 4 },
      { name: 'Feb', start: 15, span: 4 }, { name: 'Mar', start: 19, span: 5 },
      { name: 'Apr', start: 24, span: 4 }, { name: 'May', start: 28, span: 4 },
      { name: 'Jun', start: 32, span: 5 }, { name: 'Jul', start: 37, span: 4 },
      { name: 'Aug', start: 41, span: 5 }, { name: 'Sep', start: 46, span: 4 },
      { name: 'Oct', start: 50, span: 3 }
    ])
  })

  it('前九十天未知仍保留月份坐标，不把空白历史变成零调用', async () => {
    const { headers, wrapper } = await calendar(days(2025, 10, 18, 365, 90))
    expect(headers.slice(0, 4)).toEqual([
      { name: 'Oct', start: 0, span: 3 }, { name: 'Nov', start: 3, span: 4 },
      { name: 'Dec', start: 7, span: 5 }, { name: 'Jan', start: 12, span: 4 }
    ])
    expect(headers.at(-1)).toEqual({ name: '', start: 51, span: 2 })
    expect(wrapper.findAll('.n-heatmap-rect')).toHaveLength(275)
    expect(wrapper.findAll('.n-heatmap__empty-cell')).toHaveLength(96)
  })

  it('全部未知仍有月份定位，跨年填充周保留空位', async () => {
    const { headers, wrapper, bodyColumns } = await calendar(days(2026, 1, 1, 31, 31))
    expect(bodyColumns).toBe(5)
    expect(headers).toEqual([{ name: '', start: 0, span: 1 }, { name: 'Jan', start: 1, span: 4 }])
    expect(wrapper.findAll('.n-heatmap-rect')).toHaveLength(0)
    expect(wrapper.findAll('.n-heatmap__empty-cell')).toHaveLength(35)
  })

  it('闰年跨年覆盖五十四周，二月闰日和十二月末列均保留', async () => {
    // 2012-01-01 是周日，日历从 2011-12-26 到 2013-01-06，共 54 周。
    const data = days(2012, 1, 1, 366)
    data[59]!.value = 29
    const { headers, wrapper, bodyColumns } = await calendar(data)
    expect(bodyColumns).toBe(54)
    expect(headers.slice(0, 4)).toEqual([
      { name: '', start: 0, span: 1 }, { name: 'Jan', start: 1, span: 5 },
      { name: 'Feb', start: 6, span: 4 }, { name: 'Mar', start: 10, span: 4 }
    ])
    expect(headers.at(-1)).toEqual({ name: 'Dec', start: 49, span: 5 })
    expect(wrapper.findAll('.n-heatmap-rect')).toHaveLength(366)
    // 闰日是周三，落在 2012-02-27 开始的第 9 列（从 0 计）。
    expect(wrapper.findAll('tbody tr')[2]!.findAll('.n-heatmap__day-cell')[9]!.get('.n-heatmap-rect').attributes('style')).toContain('#216e39')
  })
})
