package com.and.setting

import android.content.Context
import android.graphics.Color
import android.text.style.ForegroundColorSpan
import androidx.core.content.ContextCompat
import com.and.R
import com.prolificinteractive.materialcalendarview.CalendarDay
import com.prolificinteractive.materialcalendarview.DayViewDecorator
import com.prolificinteractive.materialcalendarview.DayViewFacade
import com.prolificinteractive.materialcalendarview.spans.DotSpan
import org.threeten.bp.DayOfWeek

class DayDecorator(context: Context): DayViewDecorator {
    var drawable = ContextCompat.getDrawable(context, R.drawable.calendar_selector)
    override fun shouldDecorate(day: CalendarDay): Boolean {
        return true
    }

    // 일자 선택 시 내가 정의한 드로어블이 적용되도록 한다
    override fun decorate(view: DayViewFacade) {
        view.setSelectionDrawable(drawable!!)
    }
}

// 선택한 달 외의 날짜 표시
class SelectedMonthDecorator(private val selectedMonth : Int): DayViewDecorator {
    override fun shouldDecorate(day: CalendarDay): Boolean {
        return day.month != selectedMonth
    }
    override fun decorate(view: DayViewFacade) {
        view.addSpan(ForegroundColorSpan(Color.GRAY))
    }
}


// 산책 날짜에 점 표시
class TimeLineDayDecorator(private val days : List<CalendarDay>): DayViewDecorator {
    override fun shouldDecorate(day: CalendarDay?): Boolean {
        return days.contains(day)
    }

    override fun decorate(view: DayViewFacade) {
        view.addSpan(DotSpan(10F, Color.BLACK))
    }
}

class SundayDecorator: DayViewDecorator {
    override fun shouldDecorate(day: CalendarDay): Boolean {
        val sunday = day.date.with(DayOfWeek.SUNDAY).dayOfMonth
        return sunday == day.day
    }

    override fun decorate(view: DayViewFacade) {
        view.addSpan(object:ForegroundColorSpan(Color.RED){})
    }
}

/* 토요일 날짜의 색상을 설정하는 클래스 */
class SaturdayDecorator: DayViewDecorator {
    override fun shouldDecorate(day: CalendarDay): Boolean {
        val saturday = day.date.with(DayOfWeek.SATURDAY).dayOfMonth
        return saturday == day.day
    }

    override fun decorate(view: DayViewFacade) {
        view.addSpan(object:ForegroundColorSpan(Color.BLUE){})
    }
}