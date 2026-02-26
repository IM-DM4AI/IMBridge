#pragma once

namespace oceanbase {

    namespace sql {

        class IMLanePlugin {
            private:
                class Impl;
                Impl* impl_;
            public:
                IMLanePlugin();
                ~IMLanePlugin();

                void Setup();

                // void CallWithArrow(arrow::Table *input_table, arrow::Table *output_table);
        };
    }
}