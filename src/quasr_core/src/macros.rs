#[macro_export]
macro_rules! set {
    ( $( $x:expr ),* ) => {  // Match zero or more comma delimited items
        {
            let mut temp_set = HashSet::new();  // Create a mutable HashSet
            $(
                temp_set.insert($x.to_owned()); // Insert each item matched into the HashSet
            )*
            temp_set // Return the populated HashSet
        }
    };
}
