use sqlparser::ast::DateTimeField;

use crate::query_creation::{query_generator::{match_next_state, QueryGenerator}, state_generator::state_choosers::StateChooser};

/// subgraph def_select_datetime_field
pub struct SelectDatetimeFieldBuilder { }

impl SelectDatetimeFieldBuilder {
    pub fn build<StC: StateChooser>(
        generator: &mut QueryGenerator<StC>
    ) -> DateTimeField {
        generator.expect_state("select_datetime_field");
        let field = match_next_state!(generator, {
            "select_datetime_field_microseconds" => DateTimeField::Microseconds,
            "select_datetime_field_milliseconds" => DateTimeField::Milliseconds,
            "select_datetime_field_second" => DateTimeField::Second,
            "select_datetime_field_minute" => DateTimeField::Minute,
            "select_datetime_field_hour" => DateTimeField::Hour,
            "select_datetime_field_day" => DateTimeField::Day,
            "select_datetime_field_isodow" => DateTimeField::Isodow,
            "select_datetime_field_week" => DateTimeField::Week,
            "select_datetime_field_month" => DateTimeField::Month,
            "select_datetime_field_quarter" => DateTimeField::Quarter,
            "select_datetime_field_year" => DateTimeField::Year,
            "select_datetime_field_isoyear" => DateTimeField::Isoyear,
            "select_datetime_field_decade" => DateTimeField::Decade,
            "select_datetime_field_century" => DateTimeField::Century,
            "select_datetime_field_millennium" => DateTimeField::Millennium,
        });
        generator.expect_state("EXIT_select_datetime_field");
        field
    }
}
